"""Pipelines, per-pipeline, Incidents and Proposals pages, plus `route()`: the single
path -> page dispatcher used by both the live server and the static build.

Unknown pipeline ids give a 404 page; filter values that match nothing give an empty state.
Nothing here writes state or reads the evaluation's ground-truth manifest.
"""
from __future__ import annotations

import datetime as dt
import urllib.parse
from html import escape as e
from pathlib import Path

import pandas as pd

from .data import (INCIDENT_FILTERS, SEVERITIES, Paths, assemble, filter_incidents, load_incidents,
                   load_pipelines, parse_incident_filters, summarize_audit)
from .governance import assemble_governance, audit_stats, audit_table, load_contracts, render_governance
from .layout import (Links, Raw, a, bars, code, fmt_int, fmt_ts, heading, not_found, page, sev, static_name,
                     status_class, table, tag)
from .render import INCIDENT_HEADERS, incident_rows, pipelines_table, render

MAX_INCIDENT_ROWS = 500
FILTER_LABEL = {"severity": "severity", "pipeline": "pipeline", "family": "check family", "date": "business date"}


def _open(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["status"] == "open"] if len(df) else df


def _vc(df: pd.DataFrame, col: str) -> dict:
    return {str(k): int(v) for k, v in df[col].value_counts().items()} if len(df) else {}


# ------------------------------------------------------------------ pipelines
def render_pipelines(d: dict, contracts: list[dict], now=None, live: bool = False) -> str:
    links = Links(live, {p["pipeline_id"] for p in d["pipelines"]})
    checks = {c["pipeline_id"]: len(c["checks"]) for c in contracts}
    props: dict = {}
    for r in d.get("audit", []):
        props[r["pipeline_id"]] = props.get(r["pipeline_id"], 0) + 1
    open_by = d["incidents"].get("by_pipeline", {})
    rows = [[a(links.pipeline(p["pipeline_id"]), p["pipeline_id"]), f"v{p['version']}",
             Raw(", ".join(f"<code>{e(x)}</code>" for x in p["sources"]) or "&mdash;"), code(p["destination"]),
             checks.get(p["pipeline_id"], 0),
             fmt_ts(p["last_loaded_at"]) if p["last_loaded_at"] else Raw('<span class="muted">n/a</span>'),
             fmt_int(p["rows"]), open_by.get(p["pipeline_id"], 0), props.get(p["pipeline_id"], 0),
             a(links.governance(p["pipeline_id"]), "contract")] for p in d["pipelines"]]
    body = heading("Pipelines", "One row per manifest in <code>manifests/</code>.") + (
        '<section class="card">' + table(
            ["pipeline", "version", "sources", "destination", "checks", "last load", "rows", "open incidents",
             "proposals", ""], rows, {4, 6, 7, 8}, empty="No manifests found.") + "</section>")
    return page("Pipelines", "pipelines", body, links, now)


def render_pipeline(pid: str, d: dict, contracts: list[dict], df: pd.DataFrame, now=None,
                    live: bool = False) -> str | None:
    """Per-pipeline page, or None when `pid` is not a manifest."""
    p = next((x for x in d["pipelines"] if x["pipeline_id"] == pid), None)
    if p is None:
        return None
    links = Links(live, {x["pipeline_id"] for x in d["pipelines"]})
    c = next((x for x in contracts if x["pipeline_id"] == pid), None) or {"checks": [], "file": ""}
    op = _open(filter_incidents(df, {"pipeline": pid}))
    op = op.sort_values(["business_date", "severity"], ascending=[False, True]) if len(op) else op
    hist = [r for r in d.get("audit", []) if r["pipeline_id"] == pid]
    sources = ", ".join(f"<code>{e(x)}</code>" for x in p["sources"]) or "&mdash;"
    contract = ('<section class="card"><h2>Contract</h2><dl class="kv">'
                f'<dt>version</dt><dd>v{e(str(p["version"]))}</dd>'
                f'<dt>sources</dt><dd>{sources}</dd>'
                f'<dt>destination</dt><dd><code>{e(p["destination"] or "-")}</code></dd>'
                f'<dt>validation checks</dt><dd>{len(c["checks"])}</dd>'
                f'<dt>manifest</dt><dd><code>manifests/{e(c["file"])}</code></dd></dl>'
                f'<p><a href="{e(links.governance(pid))}">Full contract on Governance &rarr;</a></p></section>')
    load = ('<section class="card"><h2>Last load</h2><dl class="kv">'
            f'<dt>last load</dt><dd>{e(fmt_ts(p["last_loaded_at"])) if p["last_loaded_at"] else "<span class=muted>n/a</span>"}</dd>'
            f'<dt>rows</dt><dd>{fmt_int(p["rows"])}</dd>'
            f'<dt>open incidents</dt><dd>{len(op)}</dd>'
            f'<dt>proposals</dt><dd>{len(hist)}</dd></dl>'
            + (f'<p class="muted">{e(p["note"])}</p>' if p["note"] else "") + "</section>")
    more = links.incidents(pipeline=pid)
    shown = op.head(MAX_INCIDENT_ROWS).to_dict("records")
    incidents = (f'<section class="card"><h2>Open incidents'
                 + (f' <a class="more" href="{e(more)}">filter in Incidents &rarr;</a>' if more else "") + "</h2>"
                 + table([h for h in INCIDENT_HEADERS if h != "pipeline"], incident_rows(shown, links, False),
                         empty="No open incidents for this pipeline.") + "</section>")
    history = ('<section class="card audit"><h2>Proposal history</h2>'
               + (audit_stats(summarize_audit(hist)) if hist else "")
               + audit_table(hist, links, empty="No proposals for this pipeline yet.") + "</section>")
    body = (heading(pid, e(p["brief"]) if p["brief"] else "")
            + f'<div class="grid two">{contract}{load}</div>' + incidents + history)
    return page(pid, "pipelines", body, links, now)


# ------------------------------------------------------------------ incidents
def _filter_form(op_all: pd.DataFrame, filters: dict, pipelines: list[str], links: Links) -> str:
    if not links.live:
        chips = "".join(
            f'<a class="chip" href="{e(links.incidents(severity=s))}"'
            f'{" aria-current=page" if filters.get("severity") == s else ""}>{e(s)}</a>' for s in SEVERITIES)
        return (f'<div class="chips"><span class="muted">Severity:</span>{chips}</div>'
                '<p class="muted">Pipeline, family and date filters need the live server '
                '(<code>python -m dashboard serve</code>).</p>')
    options = {"severity": list(SEVERITIES),
               "pipeline": sorted(set(pipelines) | set(_vc(op_all, "pipeline_id"))),
               "family": sorted(_vc(op_all, "check_family")),
               "date": sorted(_vc(op_all, "business_date"), reverse=True)}
    fields = []
    for k in INCIDENT_FILTERS:
        vals = options[k]
        cur = filters.get(k, "")
        if cur and cur not in vals:
            vals = [cur] + vals
        opts = '<option value="">any</option>' + "".join(
            f'<option value="{e(v)}"{" selected" if v == cur else ""}>{e(v)}</option>' for v in vals)
        fields.append(f'<label>{e(FILTER_LABEL[k])}<select name="{k}">{opts}</select></label>')
    return (f'<form class="filters" method="get" action="/incidents">{"".join(fields)}'
            '<button class="secondary" type="submit">Apply</button></form>')


def _active_filters(filters: dict, links: Links) -> str:
    if not filters:
        return ""
    chips = []
    for k, v in filters.items():
        rest = {x: y for x, y in filters.items() if x != k}
        href = links.incidents(**rest)
        chips.append(f'<a class="chip" aria-current=page href="{e(href or links.incidents())}" '
                     f'title="remove this filter">{e(FILTER_LABEL[k])}: {e(v)} &times;</a>')
    return (f'<div class="chips"><span class="muted">Active filters:</span>{"".join(chips)}'
            f'<a href="{e(links.incidents())}">clear</a></div>')


def render_incidents(df: pd.DataFrame, filters: dict, pipelines: list[str], now=None, live: bool = False) -> str:
    links = Links(live, set(pipelines))
    op_all = _open(df)
    op = filter_incidents(op_all, filters)
    problems = []
    if filters.get("severity") and filters["severity"] not in SEVERITIES:
        problems.append(f"Unknown severity “{filters['severity']}” (use {', '.join(SEVERITIES)}).")
    op = op.sort_values(["business_date", "severity"], ascending=[False, True]) if len(op) else op
    closed = filter_incidents(df[df["status"] != "open"] if len(df) else df, filters)
    closed = closed.sort_values(["business_date", "detected_at"], ascending=False).head(10) if len(closed) else closed

    def add(k):
        return lambda v: links.incidents(**{**filters, k: v})

    sev_counts = {s: int((op["severity"] == s).sum()) if len(op) else 0 for s in SEVERITIES}
    shown = op.head(MAX_INCIDENT_ROWS).to_dict("records")
    body = [heading("Incidents", f"{len(op_all)} open of {len(df)} recorded."),
            '<section class="card">', _filter_form(op_all, filters, pipelines, links),
            _active_filters(filters, links),
            "".join(f'<p class="warn">{e(x)}</p>' for x in problems), "</section>",
            '<div class="grid two">',
            f'<section class="card"><h2>By severity</h2>{bars(sev_counts if len(op) else {}, add("severity"), "No open incidents for this filter.")}</section>',
            f'<section class="card"><h2>By family</h2>{bars(dict(sorted(_vc(op, "check_family").items(), key=lambda kv: -kv[1])), add("family"), "No open incidents for this filter.")}</section>',
            "</div>",
            f'<section class="card"><h2>Open incidents <span class="more muted">{len(op)} matching'
            + (f", first {MAX_INCIDENT_ROWS} shown" if len(op) > MAX_INCIDENT_ROWS else "") + "</span></h2>",
            table(INCIDENT_HEADERS + ["detected"],
                  [r + [Raw(f'<span class="nowrap">{e(fmt_ts(x["detected_at"]))}</span>')]
                   for r, x in zip(incident_rows(shown, links), shown)],
                  empty="No open incidents for this filter."), "</section>",
            '<section class="card"><h2>Recently resolved / closed</h2>',
            table(["date", "status", "pipeline", "check", "id"],
                  [[r["business_date"], tag(r["status"]), a(links.pipeline(r["pipeline_id"]), r["pipeline_id"]),
                    code(r["check_id"]), code(r["incident_id"])] for r in closed.to_dict("records")],
                  empty="Nothing resolved for this filter."), "</section>"]
    return page("Incidents", "incidents", "".join(body), links, now)


# ------------------------------------------------------------------ proposals
def render_proposals(audit: list[dict], status: str, pipelines: list[str], now=None, live: bool = False) -> str:
    links = Links(live, set(pipelines))
    counts: dict = {}
    for r in audit:
        counts[r["status"]] = counts.get(r["status"], 0) + 1
    chips = [f'<a class="chip" href="{e(links.proposals())}"{" aria-current=page" if not status else ""}>'
             f'all ({len(audit)})</a>']
    for k in sorted(counts):
        chips.append(f'<a class="chip" href="{e(links.proposals(k))}"{" aria-current=page" if k == status else ""}>'
                     f'<span class="{status_class(k)}">{e(k)}</span> ({counts[k]})</a>')
    rows = [r for r in audit if r["status"] == status] if status else audit
    empty = f"No proposals with status “{status}”." if status else "No proposals yet."
    body = (heading("Proposals", "Every healing proposal ever created, newest first. Same rows as the "
                    f'<a href="{e(links.governance())}">Governance</a> audit trail.')
            + f'<section class="card"><div class="chips">{"".join(chips)}</div>'
            + (f'<p><a href="{e(links.proposals())}">clear</a> the status filter.</p>' if status else "")
            + audit_stats(summarize_audit(rows)) + "</section>"
            + '<section class="card audit">' + audit_table(rows, links, empty=empty) + "</section>")
    return page("Proposals", "proposals", body, links, now)


# ------------------------------------------------------------------ dispatcher
def _q(query: dict, k: str) -> str:
    v = query.get(k) or ""
    return (v[0] if v else "") if isinstance(v, list) else v


def route(paths: Paths, path: str, query: dict | None = None, live: bool = True, now: dt.datetime | None = None,
          flash: str = "", flash_error: bool = False) -> tuple[int, str]:
    """(status, html) for a logical path; unknown paths and pipeline ids give a 404 page."""
    query = query or {}
    path = urllib.parse.unquote(path.rstrip("/")) or "/"
    if path in ("/", "/index.html"):
        return 200, render(assemble(paths), now, live, flash, flash_error)
    if path == "/governance":
        return 200, render_governance(assemble_governance(paths), now, live, open_pid=_q(query, "open"))
    if path == "/incidents":
        pipes = [p["pipeline_id"] for p in load_pipelines(paths.manifests, paths.warehouse)]
        return 200, render_incidents(load_incidents(paths.incidents), parse_incident_filters(query), pipes, now, live)
    d = assemble(paths)
    pipes = [p["pipeline_id"] for p in d["pipelines"]]
    if path == "/proposals":
        return 200, render_proposals(d["audit"], _q(query, "status").strip(), pipes, now, live)
    if path == "/pipelines":
        return 200, render_pipelines(d, load_contracts(paths.manifests), now, live)
    if path.startswith("/pipeline/"):
        pid = path[len("/pipeline/"):]
        html = render_pipeline(pid, d, load_contracts(paths.manifests), load_incidents(paths.incidents), now, live)
        if html is not None:
            return 200, html
        return 404, not_found(f"No pipeline “{pid}”. Known pipelines: {', '.join(pipes) or 'none'}.",
                              Links(live), now)
    return 404, not_found(f"No page at {path}.", Links(live), now)


def static_routes(paths: Paths) -> dict[str, tuple[str, dict]]:
    """{file name: (logical path, query)} for every page that can be static."""
    d = assemble(paths)
    out = {"index.html": ("/", {}), "pipelines.html": ("/pipelines", {}), "incidents.html": ("/incidents", {}),
           "proposals.html": ("/proposals", {}), "governance.html": ("/governance", {})}
    for p in d["pipelines"]:
        out[f"pipeline-{static_name(p['pipeline_id'])}.html"] = (f"/pipeline/{p['pipeline_id']}", {})
    for s in SEVERITIES:
        out[f"incidents-{static_name(s)}.html"] = ("/incidents", {"severity": s})
    for s in sorted({r["status"] for r in d["audit"]} | {"READY_FOR_APPROVAL"}):
        out[f"proposals-{static_name(s)}.html"] = ("/proposals", {"status": s})
    return out


def build_site(paths: Paths, now: dt.datetime | None = None) -> list[Path]:
    paths.site.mkdir(parents=True, exist_ok=True)
    written = []
    for name, (path, query) in static_routes(paths).items():
        _, html = route(paths, path, query, live=False, now=now)
        out = paths.site / name
        out.write_text(html)
        written.append(out)
    return written
