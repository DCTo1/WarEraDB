"""HTTP handler + route table for the viewer.

Routing is a dict mapping path → page function (all pages take a parsed
query-params dict and return an HTML string). /timer is the only JSON route.
"""

import json
import sys
from http.server import BaseHTTPRequestHandler
from urllib.parse import parse_qs

from .pages import (
    page_battle, page_battles, page_bounties, page_countries, page_overview,
    page_sql, page_stats, page_user, page_users,
)
from .updater import page_update_status, timer_state

ROUTES = {
    "/": page_overview,
    "/overview": page_overview,
    "/battles": page_battles,
    "/battle": page_battle,
    "/users": page_users,
    "/user": page_user,
    "/bounties": page_bounties,
    "/countries": page_countries,
    "/stats": page_stats,
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
            page = ROUTES.get(path)
            if page is None:
                self.send_response(404)
                self.end_headers()
                self.wfile.write(b"not found")
                return
            body = page(q)
            data = body.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        except Exception as exc:  # keep the server alive on any page error
            self.send_response(500)
            self.end_headers()
            self.wfile.write(str(exc).encode())

    def log_message(self, format: str, *args: object) -> None:
        sys.stderr.write("  %s\n" % (format % args))
