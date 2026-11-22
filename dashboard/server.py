"""Live dashboard server (stdlib only, localhost): rebuild per request + approve/reject POSTs.

POST /approve  proposal_id           -> healing.proposals.apply_proposal   (== `healing apply`)
POST /reject   proposal_id, reason   -> healing.proposals.reject_proposal  (== `healing reject`)
Both redirect (303) back to /?msg=... so the result shows as a flash message.
"""
from __future__ import annotations

import http.server
import urllib.parse

from healing.proposals import apply_proposal, reject_proposal
from healing.settings import Settings

from .data import Paths, assemble
from .render import render


def make_handler(paths: Paths, settings: Settings):
    class Handler(http.server.BaseHTTPRequestHandler):
        server_version = "PipelineAgentDashboard"

        def log_message(self, fmt, *args):          # quiet: the orchestrator terminal is the demo log
            if self.command == "POST":
                super().log_message(fmt, *args)

        def _send(self, code: int, body: bytes, ctype="text/html; charset=utf-8", headers=()):
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            for k, v in headers:
                self.send_header(k, v)
            self.end_headers()
            self.wfile.write(body)

        def _redirect(self, msg: str, err: bool = False):
            q = urllib.parse.urlencode({"msg": msg, **({"err": "1"} if err else {})})
            self._send(303, b"", headers=[("Location", f"/?{q}")])

        def do_GET(self):
            url = urllib.parse.urlparse(self.path)
            if url.path not in ("/", "/index.html"):
                return self._send(404, b"not found", "text/plain")
            q = urllib.parse.parse_qs(url.query)
            page = render(assemble(paths), live=True, flash=(q.get("msg") or [""])[0],
                          flash_error="err" in q)
            self._send(200, page.encode())

        def do_POST(self):
            origin = self.headers.get("Origin")
            if origin and urllib.parse.urlparse(origin).netloc != self.headers.get("Host"):
                return self._send(403, b"cross-origin POST refused", "text/plain")
            if self.path not in ("/approve", "/reject"):
                return self._send(404, b"not found", "text/plain")
            n = int(self.headers.get("Content-Length") or 0)
            form = urllib.parse.parse_qs(self.rfile.read(n).decode(), keep_blank_values=True)
            pid = (form.get("proposal_id") or [""])[0].strip()
            if not pid:
                return self._redirect("missing proposal_id", True)
            try:
                if self.path == "/approve":
                    files = apply_proposal(settings, pid)
                    return self._redirect(f"Applied {pid}: {', '.join(files)}. "
                                          "Incident -> resolved_pending_verification.")
                reason = (form.get("reason") or [""])[0].strip()
                if not reason:
                    return self._redirect("A rejection reason is required.", True)
                reject_proposal(settings, pid, reason)
                self._redirect(f"Rejected {pid}.")
            except SystemExit as ex:               # healing code signals refusals this way
                self._redirect(f"Refused: {ex}", True)
            except Exception as ex:
                self._redirect(f"Error: {type(ex).__name__}: {ex}", True)

    return Handler


def make_server(paths: Paths, settings: Settings, port: int = 8099) -> http.server.ThreadingHTTPServer:
    return http.server.ThreadingHTTPServer(("127.0.0.1", port), make_handler(paths, settings))
