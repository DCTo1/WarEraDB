"""HTTP handler + route table for the viewer.

Routing is a dict mapping path → page function (all pages take a parsed
query-params dict and return an HTML string). /timer and /search are the
only JSON routes (handled before the page lookup).
"""

import json
import sys
import time
from http.server import BaseHTTPRequestHandler
from urllib.parse import parse_qs

from . import usage
from .pages import (
    page_battle, page_battles, page_bounties, page_countries, page_overview,
    page_sql, page_stats, page_tracker, page_transactions,
    page_transactions_coverage, page_user, page_users, page_usage, page_weekly,
)
from .search import search
from .updater import page_update_status, timer_state

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
    "/bounties": page_bounties,
    "/countries": page_countries,
    "/stats": page_stats,
    "/usage": page_usage,
    "/sql": page_sql,
    "/update-status": page_update_status,
}


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        path, _, qs = self.path.partition("?")
        q = parse_qs(qs)
        try:
            if path == "/timer":
                payload = json.dumps(timer_state()).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return
            if path == "/search":
                payload = json.dumps(search(q.get("q", [""])[0])).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
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
