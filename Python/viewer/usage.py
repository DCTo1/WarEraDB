"""In-memory usage stats for the viewer (stdlib only, /proc on Linux).

Tracks per-route request counts / latency / bytes / errors, unique visitors
(via the Cf-Connecting-Ip header cloudflared tunnels set, falling back to the
socket address), the viewer process's RSS + CPU time (from /proc/self), and —
when docker is reachable — the TimescaleDB container's CPU/memory (short-TTL
cache, degrades to "" silently). /timer polls are excluded from the counters:
the header timer fires once per second per open page and would drown out
real page views.
"""

import os
import subprocess
import threading
import time
from datetime import datetime, timezone

START_MONO = time.monotonic()
START_WALL = datetime.now(timezone.utc)

DOCKER_TTL = 8.0

_lock = threading.Lock()
_routes: dict[str, dict] = {}  # path -> {hits, ms, ms_max, bytes, errs}
_visitors: dict[str, list[float]] = {}  # ip -> [last_seen, ...] (last 2 kept)
_hits = _errs = 0
_ms = _bytes = 0

_cpu_last: tuple[float, float] | None = None
_docker_cache: tuple[float, str] | None = None
_docker_lock = threading.Lock()
_docker_busy = False


def record(path: str, ms: float, size: int, ip: str, *, err: bool = False) -> None:
    """Record one served request. /timer is the header poll — not a page view."""
    if path == "/timer":
        return
    with _lock:
        global _hits, _errs, _ms, _bytes
        _hits += 1
        _ms += ms
        _bytes += size
        if err:
            _errs += 1
        r = _routes.setdefault(path, {"hits": 0, "ms": 0.0, "ms_max": 0.0,
                                      "bytes": 0, "errs": 0})
        r["hits"] += 1
        r["ms"] += ms
        r["bytes"] += size
        r["ms_max"] = max(r["ms_max"], ms)
        if err:
            r["errs"] += 1
        seen = _visitors.setdefault(ip, [])
        seen.append(time.time())
        if len(seen) > 2:
            seen.pop(0)


def _proc_stats() -> tuple[float, float, int]:
    """(rss_mb, cpu_seconds, thread_count) for the current process."""
    rss_mb = 0.0
    cpu_s = 0.0
    threads = 0
    try:
        page = os.sysconf("SC_PAGESIZE")
        with open("/proc/self/statm") as f:
            rss_mb = int(f.read().split()[1]) * page / 1048576.0
    except (OSError, ValueError, IndexError):
        pass
    try:
        clk = os.sysconf("SC_CLK_TCK")
        with open("/proc/self/stat") as f:
            fields = f.read().split()
        cpu_s = (int(fields[13]) + int(fields[14])) / clk  # utime + stime
    except (OSError, ValueError, IndexError):
        pass
    try:
        threads = len(os.listdir("/proc/self/task"))
    except OSError:
        pass
    return rss_mb, cpu_s, threads


def _docker_refresh() -> None:
    """Shell out to docker and store the result. Runs on a worker thread only."""
    global _docker_cache, _docker_busy
    text = ""
    try:
        names = subprocess.run(
            ["docker", "ps", "--filter", "name=timescale", "--format", "{{.Names}}"],
            capture_output=True, text=True, timeout=5).stdout.split()
        if names:
            text = subprocess.run(
                ["docker", "stats", "--no-stream", "--format",
                 "{{.Name}}: CPU {{.CPUPerc}}, mem {{.MemUsage}} ({{.MemPerc}})",
                 names[0]],
                capture_output=True, text=True, timeout=10).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        pass
    with _docker_lock:
        _docker_cache = (time.monotonic(), text)
        _docker_busy = False


def _docker_stats() -> str:
    """CPU/mem line for the timescale container — cached, never blocks.

    `docker ps` + `docker stats --no-stream` take ~1 s together, which used to
    be the single largest chunk of a /usage render: the page paid it on its
    first load and again every DOCKER_TTL while it meta-refreshes. Serve the
    cached line and refresh it on a daemon thread instead — the fresh value
    lands on the next auto-refresh, and the empty first reading renders as the
    "docker not reachable" note the page already has.
    """
    global _docker_busy
    with _docker_lock:
        cached = _docker_cache
        stale = not cached or time.monotonic() - cached[0] >= DOCKER_TTL
        start = stale and not _docker_busy
        if start:
            _docker_busy = True
    if start:
        threading.Thread(target=_docker_refresh, daemon=True).start()
    return cached[1] if cached else ""


def snapshot() -> dict:
    """All current stats as one dict (thread-safe; CPU% since last call)."""
    global _cpu_last
    now = time.monotonic()
    rss_mb, cpu_s, threads = _proc_stats()
    cpu_pct = 0.0
    if _cpu_last:
        dt = now - _cpu_last[0]
        if dt > 0:
            cpu_pct = (cpu_s - _cpu_last[1]) / dt * 100.0
    _cpu_last = (now, cpu_s)

    with _lock:
        routes = sorted(
            (dict(r, path=p) for p, r in _routes.items()),
            key=lambda r: r["hits"], reverse=True)
        now_ts = time.time()
        vis_all = len(_visitors)
        vis_24h = sum(1 for v in _visitors.values()
                      if v[-1] > now_ts - 86400)
        hits, errs, ms, bytes_ = _hits, _errs, _ms, _bytes

    return {
        "uptime": now - START_MONO,
        "started": START_WALL,
        "hits": hits,
        "errs": errs,
        "ms": ms,
        "bytes": bytes_,
        "routes": routes,
        "visitors_24h": vis_24h,
        "visitors_all": vis_all,
        "rss_mb": rss_mb,
        "cpu_sec": cpu_s,
        "cpu_pct": cpu_pct,
        "threads": threads,
        "docker": _docker_stats(),
    }
