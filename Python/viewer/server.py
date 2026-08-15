"""HTTP handler + route table for the viewer.

Routing is a dict mapping path → page function (all pages take a parsed
query-params dict and return an HTML string). /timer and /search are the
only JSON routes, and /timer/stream + /update-status/stream the only SSE
routes (all handled before the page lookup).

Page cache (2026-08-08): the heavy user pages are TTL-cached per URL (30 s)
so repeated views — and the requests of concurrent users — mostly skip the
DB. This matters because the 15 s auto-updater's write bursts stall reads
for ~1 s (measured p99 ~1.1 s at 10-25 users); cached pages ride out the
burst. Errors are never cached. Operator pages (/stats, /usage, /update-status,
/sql, /tx-priority — the last one also WRITES, so a cached render would show
a stale list) and live pages (/battle, /timer, /search) stay uncached.
"""

import json
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler
from time import monotonic
from typing import Iterator
from urllib.parse import parse_qs

from . import usage
from .pages import (
    page_battle, page_battles, page_bounties, page_countries, page_overview,
    page_snipes, page_sql, page_stats, page_tracker, page_transactions,
    page_transactions_coverage, page_tx_priority, page_user, page_users,
    page_usage, page_weekly,
)
from .search import search
from .updater import log_events, page_update_status, timer_events, timer_state

ROUTES = {
    "/": page_overview,
    "/overview": page_overview,
    "/battles": page_battles,
    "/battle": page_battle,
    "/users": page_users,
    "/user": page_user,
    "/tracker": page_tracker,
    "/weekly": page_weekly,
    "/transactions": page_transactions,
    "/transactions/coverage": page_transactions_coverage,
    "/snipes": page_snipes,
    "/bounties": page_bounties,
    "/countries": page_countries,
    "/stats": page_stats,
    "/usage": page_usage,
    "/sql": page_sql,
    "/tx-priority": page_tx_priority,
    "/update-status": page_update_status,
}

# Concurrent SSE streams. ThreadingHTTPServer gives every connection its own
# thread and a stream holds it for the life of the tab, so this caps how many
# threads abandoned background tabs can pin (they are only reaped when a
# heartbeat write fails). 200 is far above the ~50 concurrent users the viewer
# is sized for; past it, clients get a 503 and fall back to polling.
SSE_MAX_STREAMS = 200
# Aborts a stream whose socket has been unwritable this long: a wedged client
# (not a closed one — that raises) would otherwise park its thread in sendall
# forever.
SSE_WRITE_TIMEOUT = 60.0
_streams = 0
_streams_lock = threading.Lock()

PAGE_TTL = 30.0
PAGE_CACHE_MAX = 400
_PAGE_CACHE: dict[str, tuple[float, str]] = {}

# Every user-facing page except the live /battle detail (users watch active
# battles — it must stay fresh) and the JSON/operator routes above.
CACHED_ROUTES = frozenset({
    "/", "/overview", "/battles", "/users", "/user", "/tracker", "/weekly",
    "/transactions", "/bounties", "/countries", "/snipes",
})


def _cache_key(path: str, qs: str) -> str:
    return f"{path}?{qs}" if qs else path


def _page_from_cache(path: str, qs: str) -> str | None:
    hit = _PAGE_CACHE.get(_cache_key(path, qs))
    if hit and monotonic() - hit[0] < PAGE_TTL:
        return hit[1]
    return None


def _page_into_cache(path: str, qs: str, body: str) -> None:
    if "<title>Error — WarEra DB</title>" in body:
        return
    now = monotonic()
    if len(_PAGE_CACHE) >= PAGE_CACHE_MAX:
        for k in [k for k, (t, _) in _PAGE_CACHE.items() if now - t >= PAGE_TTL]:
            del _PAGE_CACHE[k]
        if len(_PAGE_CACHE) >= PAGE_CACHE_MAX:
            _PAGE_CACHE.clear()
    _PAGE_CACHE[_cache_key(path, qs)] = (now, body)


class Handler(BaseHTTPRequestHandler):
    def _serve_json(self, payload: dict | list) -> None:
        data = json.dumps(payload).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _serve_sse(self, events: Iterator[str]) -> None:
        """Serve an SSE generator until the client goes away.

        The server speaks HTTP/1.0 (BaseHTTPRequestHandler.protocol_version is
        left at the stdlib default), so the stream is close-delimited: no
        Content-Length, no chunked framing, EventSource reads until the socket
        drops. Do NOT switch the handler to HTTP/1.1 to "do it properly" — the
        404/500 paths below write bodies without a Content-Length and would
        hang a keep-alive client.

        X-Accel-Buffering/no-transform are for the proxies in front (nginx,
        cloudflared): without them a buffering intermediary holds frames back
        and the countdown freezes."""
        global _streams
        with _streams_lock:
            if _streams >= SSE_MAX_STREAMS:
                self.send_response(503)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            _streams += 1
        try:
            self.connection.settimeout(SSE_WRITE_TIMEOUT)
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-cache, no-transform")
            self.send_header("X-Accel-Buffering", "no")
            self.end_headers()
            for frame in events:
                self.wfile.write(frame.encode())
                self.wfile.flush()
        except Exception:
            # Closed tab (BrokenPipe/ConnectionReset), a wedged socket
            # (timeout), or a generator error: the response is already
            # committed, so there is nothing to report — just free the thread.
            pass
        finally:
            with _streams_lock:
                _streams -= 1

    def do_GET(self):
        path, _, qs = self.path.partition("?")
        q = parse_qs(qs)
        try:
            if path == "/timer/stream":
                self._serve_sse(timer_events())
                return
            if path == "/update-status/stream":
                self._serve_sse(log_events())
                return
            if path == "/timer":
                self._serve_json(timer_state())
                return
            if path == "/search":
                self._serve_json(search(q.get("q", [""])[0]))
                return
            page = ROUTES.get(path)
            if page is None:
                self.send_response(404)
                self.end_headers()
                self.wfile.write(b"not found")
                return
            ip = (self.headers.get("Cf-Connecting-Ip")
                  or self.headers.get("X-Forwarded-For", "").split(",")[0].strip()
                  or self.client_address[0])
            t0 = time.perf_counter()
            try:
                if path in CACHED_ROUTES:
                    body = _page_from_cache(path, qs)
                    if body is None:
                        body = page(q)
                        _page_into_cache(path, qs, body)
                else:
                    body = page(q)
                ms = (time.perf_counter() - t0) * 1000
                data = body.encode()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
                usage.record(path, ms, len(data), ip)
            except Exception as exc:  # keep the server alive on any page error
                ms = (time.perf_counter() - t0) * 1000
                usage.record(path, ms, 0, ip, err=True)
                self.send_response(500)
                self.end_headers()
                self.wfile.write(str(exc).encode())
        except Exception as exc:  # /timer, /search, 404 — keep the server alive
            self.send_response(500)
            self.end_headers()
            self.wfile.write(str(exc).encode())

    def log_message(self, format: str, *args: object) -> None:
        sys.stderr.write("  %s\n" % (format % args))
