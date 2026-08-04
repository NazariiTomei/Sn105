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
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Optional

import httpx
import uvicorn
from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse

ORCHESTRATOR_URL = os.environ.get("DASHBOARD_ORCHESTRATOR_URL", "http://localhost:8000")
WORKER_COUNT = int(os.environ.get("DASHBOARD_WORKER_COUNT", "10"))
WORKER_ENV_DIR = Path(os.environ.get("DASHBOARD_WORKER_ENV_DIR", "/workspace/beam/neurons/worker"))
REFRESH_SECONDS = float(os.environ.get("DASHBOARD_REFRESH_SECONDS", "4.0"))
JOURNAL_LINES = int(os.environ.get("DASHBOARD_JOURNAL_LINES", "400"))
STATIC_DIR = Path(__file__).parent / "static"

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


def _read_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value.strip()
    return values


async def _run(cmd: list[str]) -> str:
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL
    )
    stdout, _ = await proc.communicate()
    return stdout.decode(errors="replace")


async def _systemd_active(unit: str) -> str:
    out = await _run(["systemctl", "is-active", unit])
    return out.strip() or "unknown"


async def _journal_lines(unit: str, n: int) -> list[dict[str, Any]]:
    out = await _run(
        ["journalctl", "-u", unit, "-n", str(n), "--no-pager", "-o", "json", "--output-fields=MESSAGE,__REALTIME_TIMESTAMP"]
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


async def _journal_identity_lines(unit: str) -> list[dict[str, Any]]:
    """Registration/identity lines (worker_id, public IP) print once per process start,
    so scan the whole unit journal via grep instead of the bounded recent-events tail --
    otherwise a long-running worker eventually pushes them out of the last N lines."""
    out = await _run(
        [
            "journalctl", "-u", unit, "--no-pager", "-o", "json",
            "--output-fields=MESSAGE,__REALTIME_TIMESTAMP",
            "-g", r"Registered:|Detected public IP:|Hotkey address:",
        ]
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

    worker_tasks = []
    for i in range(1, WORKER_COUNT + 1):
        worker_tasks.append(_build_worker(i))
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
    env = _read_env(WORKER_ENV_DIR / f"{name}.env")

    active, entries, identity_entries = await asyncio.gather(
        _systemd_active(unit),
        _journal_lines(unit, JOURNAL_LINES),
        _journal_identity_lines(unit),
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


@app.get("/")
async def index():
    return FileResponse(STATIC_DIR / "index.html")


def main() -> None:
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("DASHBOARD_PORT", "5000")), log_level="info")


if __name__ == "__main__":
    main()
