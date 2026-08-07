"""
BEAM Fleet Dashboard

Polls the local orchestrator's HTTP API and each beam-workerN systemd unit's
journal to build a live snapshot of the fleet, served as JSON + a static UI.

Not part of the beam package -- a standalone read-only monitoring tool for
this VPS's deployment. Run with: python app.py (serves on :5000).
"""

import asyncio
import json
import os
import re
import shlex
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import httpx
import uvicorn
from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse

ORCHESTRATOR_URL = os.environ.get("DASHBOARD_ORCHESTRATOR_URL", "http://localhost:8000")
# Local worker{N} indices to poll. DASHBOARD_LOCAL_WORKERS takes an explicit comma-separated list
# (e.g. "2,4,7") for non-contiguous sets; falls back to DASHBOARD_WORKER_COUNT as a plain 1..N
# range for backward compatibility.
_local_workers_raw = os.environ.get("DASHBOARD_LOCAL_WORKERS", "").strip()
if _local_workers_raw:
    LOCAL_WORKER_INDICES = [int(x) for x in _local_workers_raw.split(",") if x.strip()]
else:
    LOCAL_WORKER_INDICES = list(range(1, int(os.environ.get("DASHBOARD_WORKER_COUNT", "10")) + 1))
WORKER_ENV_DIR = Path(os.environ.get("DASHBOARD_WORKER_ENV_DIR", "/workspace/beam/neurons/worker"))
REFRESH_SECONDS = float(os.environ.get("DASHBOARD_REFRESH_SECONDS", "4.0"))
JOURNAL_LINES = int(os.environ.get("DASHBOARD_JOURNAL_LINES", "400"))
STATIC_DIR = Path(__file__).parent / "static"
SSH_TIMEOUT = 8

# Workers on other VPS hosts, polled over SSH alongside the local systemd units. Configured via a
# JSON file (list of {name, host, unit, env_path, ssh_key, ssh_user?}) so adding a host doesn't
# require touching code -- see remote_workers.json next to this file.
REMOTE_WORKERS_FILE = Path(
    os.environ.get("DASHBOARD_REMOTE_WORKERS_FILE", str(Path(__file__).parent / "remote_workers.json"))
)


def _load_remote_workers() -> list[dict[str, str]]:
    if not REMOTE_WORKERS_FILE.exists():
        return []
    try:
        return json.loads(REMOTE_WORKERS_FILE.read_text())
    except (json.JSONDecodeError, OSError):
        return []

_snapshot: dict[str, Any] = {"generated_at": 0, "orchestrator": {}, "gateway": {}, "workers": [], "totals": {}, "recent_events": []}
_snapshot_lock = asyncio.Lock()


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(_refresh_loop())
    yield
    task.cancel()


app = FastAPI(title="BEAM Fleet Dashboard", lifespan=lifespan)

CHUNK_RE = re.compile(
    r"\[Worker\] Chunk (?P<chunk>\d+): (?P<bytes>\d+) bytes transferred(?P<streamed> \(streamed\))? "
    r"task=(?P<task>\S+) offer=(?P<offer>\S+) "
    r"(?:fetch_ms=(?P<fetch_ms>[\d.]+) )?"
    r"(?:hash_ms=(?P<hash_ms>[\d.]+) )?"
    r"(?:send_ms=(?P<send_ms>[\d.]+) )?"
    r"total_ms=(?P<total_ms>[\d.]+) mbps=(?P<mbps>[\d.]+) response=(?P<response>\d+)"
)
TASK_RESULT_RE = re.compile(
    r"\[WS\] Task (?P<task>\S+) offer=(?P<offer>\S+?): (?P<status>OK|FAIL)(?:: (?P<reason>.*?))? \| (?P<bytes>\d+) bytes"
)
REGISTERED_RE = re.compile(r"\[Worker\] Registered: (?P<worker_id>\S+)")
HOTKEY_ADDR_RE = re.compile(r"^Hotkey address: (?P<addr>\S+)")
PUBLIC_IP_RE = re.compile(r"\[Worker\] Detected public IP: (?P<ip>\S+)")


async def _read_env(path: str, remote: Optional[dict[str, str]] = None) -> dict[str, str]:
    if remote:
        text = await _run(["cat", path], remote=remote)
    else:
        p = Path(path)
        text = p.read_text() if p.exists() else ""
    values: dict[str, str] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value.strip()
    return values


async def _run(cmd: list[str], remote: Optional[dict[str, str]] = None) -> str:
    if remote:
        ssh_cmd = [
            "ssh", "-i", remote["ssh_key"],
            "-o", "BatchMode=yes", "-o", f"ConnectTimeout={SSH_TIMEOUT}",
            "-o", "StrictHostKeyChecking=accept-new",
            f"{remote.get('ssh_user', 'root')}@{remote['host']}",
            "--",
            *cmd,
        ]
        proc = await asyncio.create_subprocess_exec(
            *ssh_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL
        )
    else:
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL
        )
    try:
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=SSH_TIMEOUT + 5)
    except asyncio.TimeoutError:
        proc.kill()
        return ""
    return stdout.decode(errors="replace")


async def _systemd_active(unit: str, remote: Optional[dict[str, str]] = None) -> str:
    out = await _run(["systemctl", "is-active", unit], remote=remote)
    return out.strip() or "unknown"


async def _journal_lines(unit: str, n: int, remote: Optional[dict[str, str]] = None) -> list[dict[str, Any]]:
    out = await _run(
        ["journalctl", "-u", unit, "-n", str(n), "--no-pager", "-o", "json", "--output-fields=MESSAGE,__REALTIME_TIMESTAMP"],
        remote=remote,
    )
    entries = []
    for line in out.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            entries.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return entries


IDENTITY_GREP_PATTERN = r"Registered:|Detected public IP:|Hotkey address:"


async def _journal_identity_lines(unit: str, remote: Optional[dict[str, str]] = None) -> list[dict[str, Any]]:
    """Registration/identity lines (worker_id, public IP) print once per process start,
    so scan the whole unit journal via grep instead of the bounded recent-events tail --
    otherwise a long-running worker eventually pushes them out of the last N lines."""
    # ssh joins a multi-arg remote command into one string and hands it to the remote shell,
    # so the unquoted "|" in the pattern gets parsed there as a real shell pipe (splitting into
    # "journalctl ... -g Registered:" | "Detected public IP:" | "Hotkey address:", the latter two
    # being nonexistent commands) instead of reaching journalctl as part of the -g argument. Quote
    # it only for the remote case -- local exec has no shell to strip the quotes back off.
    pattern = shlex.quote(IDENTITY_GREP_PATTERN) if remote else IDENTITY_GREP_PATTERN
    out = await _run(
        [
            "journalctl", "-u", unit, "--no-pager", "-o", "json",
            "--output-fields=MESSAGE,__REALTIME_TIMESTAMP",
            "-g", pattern,
        ],
        remote=remote,
    )
    entries = []
    for line in out.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            entries.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return entries


# PM2 doesn't unbuffer or timestamp output by default -- workers started under PM2 for this
# dashboard must run with `python -u` and `pm2 start --time`, which prefixes each log line with
# "YYYY-MM-DDTHH:MM:SS: ". Parsed the same way journalctl's JSON entries are, so the rest of the
# pipeline (_parse_worker_journal, _parse_identity) doesn't need to know the source differs.
PM2_LOG_LINE_RE = re.compile(r"^(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}): ?(?P<msg>.*)$")


def _parse_pm2_log_text(text: str) -> list[dict[str, Any]]:
    entries = []
    for line in text.splitlines():
        m = PM2_LOG_LINE_RE.match(line)
        if not m:
            continue
        try:
            dt = datetime.strptime(m.group("ts"), "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
        except ValueError:
            continue
        entries.append({"MESSAGE": m.group("msg"), "__REALTIME_TIMESTAMP": str(int(dt.timestamp() * 1_000_000))})
    return entries


def _normalize_status(status: str) -> str:
    """Map process-manager-specific status vocabularies onto systemd's, since that's the
    contract the frontend and totals already use ("active" = running)."""
    return "active" if status == "online" else status


async def _pm2_active(name: str, remote: dict[str, str]) -> str:
    out = await _run(["pm2", "jlist"], remote=remote)
    try:
        procs = json.loads(out)
    except json.JSONDecodeError:
        return "unknown"
    for p in procs:
        if p.get("name") == name:
            return _normalize_status(p.get("pm2_env", {}).get("status", "unknown"))
    return "unknown"


async def _pm2_log_lines(name: str, remote: dict[str, str], n: int) -> list[dict[str, Any]]:
    out = await _run(["tail", "-n", str(n), f"/root/.pm2/logs/{name}-out.log"], remote=remote)
    return _parse_pm2_log_text(out)


async def _pm2_identity_lines(name: str, remote: dict[str, str]) -> list[dict[str, Any]]:
    # Same ssh remote-command-joining issue as _journal_identity_lines -- quote the pattern so
    # the unescaped "|" doesn't get parsed as a shell pipe on the remote end.
    pattern = shlex.quote(IDENTITY_GREP_PATTERN) if remote else IDENTITY_GREP_PATTERN
    out = await _run(
        ["grep", "-aE", pattern, f"/root/.pm2/logs/{name}-out.log"],
        remote=remote,
    )
    return _parse_pm2_log_text(out)


def _parse_identity(entries: list[dict[str, Any]]) -> dict[str, Optional[str]]:
    worker_id = None
    public_ip = None
    hotkey_addr = None
    for entry in entries:
        msg = entry.get("MESSAGE")
        if not isinstance(msg, str):
            continue
        m = REGISTERED_RE.search(msg)
        if m:
            worker_id = m.group("worker_id")
        m = PUBLIC_IP_RE.search(msg)
        if m:
            public_ip = m.group("ip")
        m = HOTKEY_ADDR_RE.search(msg)
        if m:
            hotkey_addr = m.group("addr")
    return {"worker_id": worker_id, "public_ip": public_ip, "hotkey_addr": hotkey_addr}


def _parse_worker_journal(worker_name: str, entries: list[dict[str, Any]]) -> dict[str, Any]:
    worker_id = None
    hotkey_addr = None
    public_ip = None
    events: list[dict[str, Any]] = []
    completed = 0
    failed = 0
    total_bytes = 0

    for entry in entries:
        msg = entry.get("MESSAGE")
        if not isinstance(msg, str):
            continue
        ts_us = entry.get("__REALTIME_TIMESTAMP")
        ts = int(ts_us) / 1_000_000 if ts_us else None

        m = REGISTERED_RE.search(msg)
        if m:
            worker_id = m.group("worker_id")
            continue

        m = PUBLIC_IP_RE.search(msg)
        if m:
            public_ip = m.group("ip")
            continue

        m = HOTKEY_ADDR_RE.search(msg)
        if m:
            hotkey_addr = m.group("addr")
            continue

        m = CHUNK_RE.search(msg)
        if m:
            g = m.groupdict()
            events.append({
                "worker": worker_name,
                "ts": ts,
                "bytes": int(g["bytes"]),
                "task": g["task"],
                "offer": g["offer"],
                "streamed": bool(g["streamed"]),
                "fetch_ms": float(g["fetch_ms"]) if g["fetch_ms"] else None,
                "hash_ms": float(g["hash_ms"]) if g["hash_ms"] else None,
                "send_ms": float(g["send_ms"]) if g["send_ms"] else None,
                "total_ms": float(g["total_ms"]),
                "mbps": float(g["mbps"]),
                "response": int(g["response"]),
            })
            continue

        m = TASK_RESULT_RE.search(msg)
        if m:
            if m.group("status") == "OK":
                completed += 1
                total_bytes += int(m.group("bytes"))
            else:
                failed += 1

    recent = events[-20:]
    mbps_vals = [e["mbps"] for e in events]
    total_ms_vals = [e["total_ms"] for e in events]
    fetch_vals = [e["fetch_ms"] for e in events if e["fetch_ms"] is not None]
    hash_vals = [e["hash_ms"] for e in events if e["hash_ms"] is not None]
    send_vals = [e["send_ms"] for e in events if e["send_ms"] is not None]

    def avg(vals: list[float]) -> Optional[float]:
        return round(sum(vals) / len(vals), 1) if vals else None

    return {
        "worker_id": worker_id,
        "hotkey_addr": hotkey_addr,
        "public_ip": public_ip,
        "tasks_completed": completed,
        "tasks_failed": failed,
        "total_bytes": total_bytes,
        "avg_mbps": avg(mbps_vals),
        "avg_total_ms": avg(total_ms_vals),
        "avg_fetch_ms": avg(fetch_vals),
        "avg_hash_ms": avg(hash_vals),
        "avg_send_ms": avg(send_vals),
        "recent_events": recent,
        "last_event_ts": recent[-1]["ts"] if recent else None,
    }


async def _fetch_orchestrator() -> dict[str, Any]:
    result: dict[str, Any] = {"reachable": False}
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            health, ready, state, gateway = await asyncio.gather(
                client.get(f"{ORCHESTRATOR_URL}/health"),
                client.get(f"{ORCHESTRATOR_URL}/ready"),
                client.get(f"{ORCHESTRATOR_URL}/state"),
                client.get(f"{ORCHESTRATOR_URL}/gateway/status"),
                return_exceptions=True,
            )
        result["reachable"] = True
        if not isinstance(health, Exception) and health.status_code == 200:
            result["health"] = health.json()
        if not isinstance(ready, Exception) and ready.status_code == 200:
            result["ready"] = ready.json()
        if not isinstance(state, Exception) and state.status_code == 200:
            result["state"] = state.json()
        if not isinstance(gateway, Exception) and gateway.status_code == 200:
            result["gateway"] = gateway.json()
    except Exception as exc:
        result["error"] = str(exc)
    return result


async def _build_snapshot() -> dict[str, Any]:
    orch_info = await _fetch_orchestrator()

    worker_tasks = [_build_worker(i) for i in LOCAL_WORKER_INDICES]
    worker_tasks += [_build_remote_worker(spec) for spec in _load_remote_workers()]
    workers = await asyncio.gather(*worker_tasks)

    all_events = []
    for w in workers:
        all_events.extend(w["recent_events"])
    all_events.sort(key=lambda e: e["ts"] or 0, reverse=True)

    totals = {
        "workers_online": sum(1 for w in workers if w["systemd_active"] == "active"),
        "workers_total": len(workers),
        "tasks_completed": sum(w["tasks_completed"] for w in workers),
        "tasks_failed": sum(w["tasks_failed"] for w in workers),
        "total_bytes": sum(w["total_bytes"] for w in workers),
    }
    mbps_all = [e["mbps"] for e in all_events]
    totals["avg_mbps"] = round(sum(mbps_all) / len(mbps_all), 1) if mbps_all else None

    for w in workers:
        w.pop("recent_events_full", None)

    return {
        "generated_at": time.time(),
        "orchestrator": orch_info,
        "workers": workers,
        "totals": totals,
        "recent_events": all_events[:25],
    }


async def _build_worker(index: int) -> dict[str, Any]:
    name = f"worker{index}"
    unit = f"beam-{name}.service"
    env = await _read_env(str(WORKER_ENV_DIR / f"{name}.env"))
    return await _assemble_worker(name, unit, env, remote=None)


async def _build_remote_worker(spec: dict[str, str]) -> dict[str, Any]:
    remote = {"host": spec["host"], "ssh_key": spec["ssh_key"], "ssh_user": spec.get("ssh_user", "root")}
    env = await _read_env(spec["env_path"], remote=remote)
    manager = spec.get("manager", "systemd")
    worker = await _assemble_worker(spec["name"], spec["unit"], env, remote=remote, manager=manager)
    worker["host"] = spec["host"]
    return worker


async def _assemble_worker(
    name: str, unit: str, env: dict[str, str], remote: Optional[dict[str, str]], manager: str = "systemd"
) -> dict[str, Any]:
    if manager == "pm2":
        assert remote is not None, "pm2 manager only supported for remote workers"
        active, entries, identity_entries = await asyncio.gather(
            _pm2_active(unit, remote),
            _pm2_log_lines(unit, remote, JOURNAL_LINES),
            _pm2_identity_lines(unit, remote),
        )
    else:
        active, entries, identity_entries = await asyncio.gather(
            _systemd_active(unit, remote=remote),
            _journal_lines(unit, JOURNAL_LINES, remote=remote),
            _journal_identity_lines(unit, remote=remote),
        )
    parsed = _parse_worker_journal(name, entries)
    identity = _parse_identity(identity_entries)
    parsed["worker_id"] = identity["worker_id"] or parsed["worker_id"]
    parsed["public_ip"] = identity["public_ip"] or parsed["public_ip"]
    parsed["hotkey_addr"] = identity["hotkey_addr"] or parsed["hotkey_addr"]

    return {
        "name": name,
        "unit": unit,
        "hotkey_name": env.get("WALLET_HOTKEY", "?"),
        "systemd_active": active,
        "stream_mode": env.get("WORKER_STREAM_UPLOADS", "false").lower() in ("1", "true", "yes"),
        "concurrency": int(env.get("WORKER_MAX_CONCURRENT_TASKS", "1")),
        **parsed,
    }


async def _refresh_loop() -> None:
    global _snapshot
    while True:
        try:
            snap = await _build_snapshot()
            async with _snapshot_lock:
                _snapshot = snap
        except Exception as exc:
            async with _snapshot_lock:
                _snapshot["error"] = str(exc)
        await asyncio.sleep(REFRESH_SECONDS)


@app.get("/api/summary")
async def api_summary():
    async with _snapshot_lock:
        return JSONResponse(_snapshot)


@app.delete("/api/workers/{name}")
async def remove_worker(name: str):
    """Stop/disable the worker on its own VPS and drop it from remote_workers.json.
    Local workers (env-var-configured, not file-backed) aren't removable this way."""
    workers = _load_remote_workers()
    match = None
    remaining = []
    for w in workers:
        if w["name"] == name:
            match = w
        else:
            remaining.append(w)

    if match is None:
        return JSONResponse(
            {"error": f"worker '{name}' not found in remote_workers.json"}, status_code=404
        )

    remote = {"host": match["host"], "ssh_key": match["ssh_key"], "ssh_user": match.get("ssh_user", "root")}
    manager = match.get("manager", "systemd")

    if manager == "pm2":
        await _run(["pm2", "stop", match["unit"]], remote=remote)
        await _run(["pm2", "delete", match["unit"]], remote=remote)
        await _run(["pm2", "save"], remote=remote)
    else:
        await _run(["systemctl", "stop", match["unit"]], remote=remote)
        await _run(["systemctl", "disable", match["unit"]], remote=remote)
        await _run(["rm", "-f", f"/etc/systemd/system/{match['unit']}"], remote=remote)
        await _run(["systemctl", "daemon-reload"], remote=remote)
    await _run(["rm", "-f", match["env_path"]], remote=remote)

    REMOTE_WORKERS_FILE.write_text(json.dumps(remaining, indent=2) + "\n")

    async with _snapshot_lock:
        _snapshot["workers"] = [w for w in _snapshot.get("workers", []) if w["name"] != name]

    return {"removed": name}


@app.get("/")
async def index():
    return FileResponse(STATIC_DIR / "index.html")


def main() -> None:
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("DASHBOARD_PORT", "5000")), log_level="info")


if __name__ == "__main__":
    main()
