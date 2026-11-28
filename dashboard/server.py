"""Live dashboard server (stdlib only, localhost): rebuild per request + approve/reject POSTs.

GET  /  /pipelines  /pipeline/<id>  /incidents  /proposals  /governance   (see pages.route)
POST /approve  proposal_id, confirm=apply -> healing.proposals.apply_proposal   (== `healing apply`);
               refused while a pipeline-generation job is active
POST /reject   proposal_id, reason        -> healing.proposals.reject_proposal  (== `healing reject`)
Both redirect (303) back to /?msg=... so the result shows as a flash message. An approve
without confirm=apply (i.e. not sent from the "Confirm apply to production" step) is refused.

Pipeline generation (live only; see jobs.py):
GET  /new                      source catalog + form
POST /generate                 sources, pipeline_id, prompt -> 303 /jobs/<id> (or the form again with errors)
GET  /jobs  /jobs/<id>         job list / live job page
GET  /jobs/<id>/poll?offset=N  JSON: status, new log text, head html (the job page polls this every 2 s)
POST /jobs/<id>/cancel         stop the job's whole process group
POST /pipeline/<id>/orchestrate  "Enable schedule and run now" for an existing pipeline -> 303 /jobs/<id>
                                 (refused while a job is active)
Every POST gets the same same-origin check.

Airflow status on pages comes from StatusCache (refreshed in a background thread): no page request
ever waits for the airflow CLI.
"""
from __future__ import annotations

import http.server
import re
import urllib.parse

from healing.proposals import apply_proposal, reject_proposal
from healing.settings import Settings

from .airflow import StatusCache
from .data import Paths
from .genpages import poll_json, render_job, render_jobs, render_new
from .jobs import GENERATING_REFUSAL, GenConfig, JobManager, Refused, active_job_on_disk
from .layout import Links, not_found
from .pages import route

MAX_BODY = 64 * 1024
_JOB = re.compile(r"^/jobs/([^/]+)(/poll|/cancel)?$")
_ORCH = re.compile(r"^/pipeline/([a-z][a-z0-9_]{1,47})/orchestrate$")


def _pipeline_ids(paths: Paths) -> set:
    import yaml
    out = set()
    for p in sorted(paths.manifests.glob("*.yaml")):
        try:
            out.add(str((yaml.safe_load(p.read_text()) or {}).get("pipeline_id", p.stem)))
        except Exception:
            continue
    return out


def make_handler(paths: Paths, settings: Settings, jobs: JobManager | None = None,
                 airflow_status: StatusCache | None = None):
    if airflow_status is None and jobs is not None:
        airflow_status = StatusCache(jobs.airflow)

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

        def _generating(self) -> dict | None:
            """The active generation job (this server's, or one recorded on disk by another process)."""
            if jobs is None:
                return None
            return jobs.active() or active_job_on_disk(jobs.cfg.jobs_dir)

        def _gen_get(self, path: str, q: dict):
            """(code, body, ctype) for /new and /jobs*, or None when the path is not one of them."""
            if jobs is None or not (path == "/new" or path == "/jobs" or path.startswith("/jobs/")):
                return None
            pipes = _pipeline_ids(paths)
            if path == "/new":
                return 200, render_new(jobs, paths.manifests, pipes), None
            if path == "/jobs":
                return 200, render_jobs(jobs, pipes), None
            m = _JOB.match(path)
            if m and m.group(2) == "/poll":
                try:
                    off = max(0, int((q.get("offset") or ["0"])[0]))
                except ValueError:
                    off = 0
                body = poll_json(jobs, m.group(1), off, pipes)
                if body is not None:
                    return 200, body, "application/json; charset=utf-8"
            elif m and not m.group(2):
                html = render_job(jobs, m.group(1), pipes)
                if html is not None:
                    return 200, html, None
            return 404, not_found("No such generation job.", Links(True)), None

        def do_GET(self):
            url = urllib.parse.urlparse(self.path)
            q = urllib.parse.parse_qs(url.query)
            try:
                gen = self._gen_get(url.path.rstrip("/") or "/", q)
                if gen is not None:
                    code, page, ctype = gen
                    return self._send(code, page.encode(), *( [ctype] if ctype else []))
                code, page = route(paths, url.path, q, live=True, flash=(q.get("msg") or [""])[0],
                                   flash_error="err" in q,
                                   generating=self._generating() if (url.path in ("/", "/index.html")
                                                                      or url.path.startswith("/pipeline/")) else None,
                                   airflow=airflow_status.get() if airflow_status is not None else None)
            except Exception as ex:                # never a traceback in the browser
                self.log_error("GET %s failed: %s: %s", url.path, type(ex).__name__, ex)
                code, page = 500, not_found("This page could not be rendered; see the server log.", Links(True))
            self._send(code, page.encode())

        def do_POST(self):
            origin = self.headers.get("Origin")
            if origin and urllib.parse.urlparse(origin).netloc != self.headers.get("Host"):
                return self._send(403, b"cross-origin POST refused", "text/plain")
            try:
                n = int(self.headers.get("Content-Length") or 0)
            except ValueError:
                n = -1
            if not 0 <= n <= MAX_BODY:
                return self._send(413, b"request body too large", "text/plain")
            form = urllib.parse.parse_qs(self.rfile.read(n).decode("utf-8", errors="replace"),
                                         keep_blank_values=True)
            if jobs is not None and (self.path == "/generate" or _JOB.match(self.path) or _ORCH.match(self.path)):
                return self._gen_post(form)
            if self.path not in ("/approve", "/reject"):
                return self._send(404, b"not found", "text/plain")
            pid = (form.get("proposal_id") or [""])[0].strip()
            if not pid:
                return self._redirect("missing proposal_id", True)
            try:
                if self.path == "/approve":
                    if (form.get("confirm") or [""])[0] != "apply":
                        return self._redirect("Approve needs the explicit confirm step "
                                              "(\u201cConfirm apply to production\u201d). Nothing was applied.", True)
                    if self._generating():
                        return self._redirect(GENERATING_REFUSAL + " Nothing was applied.", True)
                    files = apply_proposal(settings, pid, channel="dashboard")
                    return self._redirect(f"Applied {pid}: {', '.join(files)}. "
                                          "Incident -> resolved_pending_verification.")
                reason = (form.get("reason") or [""])[0].strip()
                if not reason:
                    return self._redirect("A rejection reason is required.", True)
                reject_proposal(settings, pid, reason, channel="dashboard")
                self._redirect(f"Rejected {pid}.")
            except SystemExit as ex:               # healing code signals refusals this way
                self._redirect(f"Refused: {ex}", True)
            except Exception as ex:
                self._redirect(f"Error: {type(ex).__name__}: {ex}", True)

        def _gen_post(self, form: dict):
            o = _ORCH.match(self.path)
            if o:
                try:
                    job_id = jobs.submit_orchestrate(o.group(1))
                except Refused as ex:
                    return self._redirect("Not started: " + " ".join(ex.args[0]), True)
                if airflow_status is not None:
                    airflow_status.invalidate()
                return self._send(303, b"", headers=[("Location", f"/jobs/{job_id}")])
            if self.path == "/generate":
                try:
                    job_id = jobs.submit(form)
                except Refused as ex:
                    busy = jobs.active() is not None
                    page = render_new(jobs, paths.manifests, _pipeline_ids(paths), form, ex.args[0])
                    return self._send(409 if busy else 400, page.encode())
                return self._send(303, b"", headers=[("Location", f"/jobs/{job_id}")])
            m = _JOB.match(self.path)
            if m and m.group(2) == "/cancel":
                try:
                    jobs.cancel(m.group(1))
                except KeyError:
                    return self._send(404, b"no such job", "text/plain")
                return self._send(303, b"", headers=[("Location", f"/jobs/{m.group(1)}")])
            return self._send(404, b"not found", "text/plain")

    return Handler


def make_server(paths: Paths, settings: Settings, port: int = 8099,
                jobs: JobManager | None = None) -> http.server.ThreadingHTTPServer:
    """Threading server: pages stay responsive while a generation job runs in the background.
    `jobs` defaults to a JobManager on the real repo, writing into the dashboard's warehouse."""
    if jobs is None:
        jobs = JobManager(GenConfig(warehouse=paths.warehouse))
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", port), make_handler(paths, settings, jobs))
    srv.daemon_threads = True
    return srv
