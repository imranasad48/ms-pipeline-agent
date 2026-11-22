"""Data -> one self-contained HTML page (inline CSS, no external assets).

Static (`build`): no scripts, no forms. Live (`serve`, live=True): approve/reject forms on
READY_FOR_APPROVAL cards and a small script that refreshes the page every 5 s (never while
you are typing a rejection reason; <noscript> falls back to a meta refresh)."""
from __future__ import annotations

import datetime as dt
import re
from html import escape as e

from .data import SEVERITIES

CSS = """
:root{--bg:#f7f8fa;--card:#fff;--ink:#1b2430;--mute:#667085;--line:#e4e7ec;--crit:#c0392b;--warn:#d98e04;--info:#3b7ddd;--ok:#1e8e5a;--bar:#98a2b3}
@media (prefers-color-scheme:dark){:root{--bg:#10151c;--card:#182029;--ink:#e6eaf0;--mute:#97a3b4;--line:#2a3541;--crit:#ef6b5b;--warn:#f0b43c;--info:#6aa5f5;--ok:#4cc38a;--bar:#5d6b7c}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:14px/1.5 -apple-system,Segoe UI,Roboto,sans-serif}
main{max-width:1100px;margin:0 auto;padding:20px 16px 60px}h1{font-size:22px;margin:0}h2{font-size:16px;margin:0 0 10px}h3{font-size:14px;margin:16px 0 6px}
.sub{color:var(--mute);margin:2px 0 18px}.grid{display:grid;gap:12px}.k{grid-template-columns:repeat(auto-fit,minmax(150px,1fr))}.two{grid-template-columns:repeat(auto-fit,minmax(300px,1fr))}
.card{background:var(--card);border:1px solid var(--line);border-radius:8px;padding:14px 16px;margin-bottom:14px}.kpi .n{font-size:26px;font-weight:600}.kpi .l{color:var(--mute);font-size:12px}
table{border-collapse:collapse;width:100%;font-size:13px}th,td{text-align:left;padding:5px 8px;border-bottom:1px solid var(--line);vertical-align:top}th{color:var(--mute);font-weight:600;font-size:12px}
.wrap{overflow-x:auto}.num{text-align:right;font-variant-numeric:tabular-nums}code{font:12px ui-monospace,Menlo,monospace}
.pill{display:inline-block;padding:1px 8px;border-radius:10px;font-size:12px;border:1px solid var(--line)}
.critical{color:var(--crit)}.warn{color:var(--warn)}.info{color:var(--info)}.ok{color:var(--ok)}.mute{color:var(--mute)}
.bar{display:flex;align-items:center;gap:8px}.bar i{display:block;height:8px;background:var(--bar);border-radius:3px;min-width:2px}
.ready{border:2px solid var(--warn);box-shadow:0 0 0 3px color-mix(in srgb,var(--warn) 25%,transparent)}
.ready h2{color:var(--warn)}pre.diff{background:var(--bg);border:1px solid var(--line);border-radius:6px;padding:8px 10px;overflow-x:auto;font:12px/1.4 ui-monospace,Menlo,monospace;margin:6px 0}
pre.diff .add{color:var(--ok)}pre.diff .del{color:var(--crit)}pre.diff .hunk{color:var(--info)}
.actions{display:flex;gap:10px;align-items:flex-start;flex-wrap:wrap;margin-top:12px}.actions form{margin:0}
button{font:inherit;border:0;border-radius:6px;padding:7px 18px;cursor:pointer;color:#fff}.approve{background:var(--ok)}.reject{background:var(--crit)}
textarea{font:inherit;width:300px;max-width:100%;height:34px;vertical-align:top;border:1px solid var(--line);border-radius:6px;padding:5px 8px;background:var(--bg);color:var(--ink)}
.flash{border-left:4px solid var(--info)}.flash.err{border-color:var(--crit)}.banner{border-left:4px solid var(--warn)}
.md h1,.md h2,.md h3{font-size:14px;margin:14px 0 6px}.md p,.md li{margin:3px 0}.md ul{padding-left:20px;margin:4px 0}
"""


def _pill(text: str, cls: str = "") -> str:
    return f'<span class="pill {e(cls)}">{e(str(text))}</span>'


def _table(headers: list, rows: list, num: set = frozenset()) -> str:
    if not rows:
        return '<p class="mute">none</p>'
    th = "".join(f"<th{' class=num' if i in num else ''}>{e(h)}</th>" for i, h in enumerate(headers))
    body = "".join("<tr>" + "".join(
        f"<td{' class=num' if i in num else ''}>{c if isinstance(c, _Raw) else e(str(c))}</td>"
        for i, c in enumerate(r)) + "</tr>" for r in rows)
    return f'<div class="wrap"><table><tr>{th}</tr>{body}</table></div>'


class _Raw(str):
    """Marks a string as already-safe HTML."""


def _bars(counts: dict) -> str:
    if not counts:
        return '<p class="mute">none</p>'
    top = max(counts.values()) or 1
    rows = [[k, _Raw(f'<div class="bar"><i style="width:{max(2, int(180 * v / top))}px"></i>{v}</div>')]
            for k, v in counts.items()]
    return _table(["", ""], rows)


def _inline(text: str) -> str:
    text = e(text)
    text = re.sub(r"`([^`]+)`", r"<code>\1</code>", text)
    return re.sub(r"\*\*([^*]+)\*\*", r"<b>\1</b>", text)


def markdown_to_html(md: str) -> str:
    """Minimal markdown: #-headings, pipe tables, '-' bullets, paragraphs, **bold**, `code`."""
    out, lines, i = [], md.splitlines(), 0
    in_list = False
    while i < len(lines):
        ln = lines[i]
        if in_list and not ln.startswith("- "):
            out.append("</ul>")
            in_list = False
        if ln.startswith("|") and i + 1 < len(lines) and re.match(r"^\|[\s:|-]+\|$", lines[i + 1]):
            cells = lambda s: [c.strip() for c in s.strip().strip("|").split("|")]
            head, i, rows = cells(ln), i + 2, []
            while i < len(lines) and lines[i].startswith("|"):
                rows.append(cells(lines[i]))
                i += 1
            out.append('<div class="wrap"><table><tr>' + "".join(f"<th>{_inline(h)}</th>" for h in head)
                       + "</tr>" + "".join("<tr>" + "".join(f"<td>{_inline(c)}</td>" for c in r) + "</tr>"
                                           for r in rows) + "</table></div>")
            continue
        m = re.match(r"^(#{1,6})\s+(.*)", ln)
        if m:
            lvl = min(len(m.group(1)) + 1, 4)
            out.append(f"<h{lvl}>{_inline(m.group(2))}</h{lvl}>")
        elif ln.startswith("- "):
            if not in_list:
                out.append("<ul>")
                in_list = True
            out.append(f"<li>{_inline(ln[2:])}</li>")
        elif ln.strip():
            out.append(f"<p>{_inline(ln.strip())}</p>")
        i += 1
    if in_list:
        out.append("</ul>")
    return "\n".join(out)


def _sev(s: str) -> _Raw:
    return _Raw(f'<span class="{e(s)}">{e(s)}</span>')


def _diff_html(diff: str) -> str:
    out = []
    for ln in diff.splitlines():
        cls = ("add" if ln.startswith("+") and not ln.startswith("+++") else
               "del" if ln.startswith("-") and not ln.startswith("---") else
               "hunk" if ln.startswith("@@") else "")
        out.append(f'<span class="{cls}">{e(ln)}</span>' if cls else e(ln))
    return '<pre class="diff">' + "\n".join(out) + "</pre>"


def _ready_card(r: dict, live: bool) -> str:
    pid = e(r["proposal_id"])
    body = [f'<section class="card ready"><h2>Awaiting your approval &mdash; <code>{pid}</code></h2>',
            f'<p class="sub">{e(r["pipeline_id"])} &middot; <code>{e(r["check_id"])}</code> &middot; '
            f'incident <code>{e(r["incident_id"])}</code> &middot; agent decision: {e(r["decision"] or "-")}</p>',
            '<h3>Diagnosis</h3>', f'<div class="md">{markdown_to_html(r["diagnosis"]) or "<p class=mute>none</p>"}</div>',
            '<h3>Proposed change</h3>', _diff_html(r["diff"]) if r["diff"] else '<p class="mute">no diff</p>',
            '<h3>Validation (before vs after)</h3>',
            f'<div class="md">{markdown_to_html(r["validation"]) or "<p class=mute>none</p>"}</div>']
    if live:
        body.append(
            f'<div class="actions"><form method="post" action="/approve">'
            f'<input type="hidden" name="proposal_id" value="{pid}">'
            f'<button class="approve" type="submit">Approve &amp; apply</button></form>'
            f'<form method="post" action="/reject">'
            f'<input type="hidden" name="proposal_id" value="{pid}">'
            f'<textarea name="reason" placeholder="Reason for rejecting" required></textarea> '
            f'<button class="reject" type="submit">Reject</button></form></div>')
    else:
        body.append(f'<p class="mute">Approve: <code>python -m healing apply {pid}</code> &middot; '
                    f'serve the live dashboard for buttons.</p>')
    return "".join(body) + "</section>"


REFRESH_JS = """<script>setInterval(function(){var a=document.activeElement;
var typing=a&&(a.tagName=='TEXTAREA'||a.tagName=='INPUT');
var drafted=[].some.call(document.querySelectorAll('textarea'),function(t){return t.value});
if(!typing&&!drafted)location.reload()},5000)</script><noscript><meta http-equiv=refresh content=5></noscript>"""


def render(d: dict, now: dt.datetime | None = None, live: bool = False, flash: str = "",
           flash_error: bool = False) -> str:
    now = now or dt.datetime.now()
    inc, props = d["incidents"], d["proposals"]
    sev = inc["by_severity"]
    orch = d.get("orchestrator") or {}
    pending = d["proposal_counts"].get("READY_FOR_APPROVAL", 0)
    kpis = [(inc["open"], "open incidents"), (sev["critical"], "critical"), (sev["warn"], "warn"),
            (sev["info"], "info"), (len(d["pipelines"]), "pipelines"), (pending, "proposals awaiting approval")]
    s = [f"<!doctype html><html lang=en><head><meta charset=utf-8>"
         f"<meta name=viewport content='width=device-width,initial-scale=1'>"
         f"<title>Pipeline Agent Dashboard</title><style>{CSS}</style>"
         f"{REFRESH_JS if live else ''}</head><body><main>",
         "<h1>Pipeline Agent</h1>",
         f"<p class=sub>Generated {e(now.isoformat(timespec='seconds'))} &middot; "
         f"read-only view of manifests, detection/incidents.db, healing/proposals and evaluation results</p>",
         *( [f'<section class="card flash{" err" if flash_error else ""}">{e(flash)}</section>']
            if flash else []),
         *( [f'<section class="card banner">Orchestrator is healing incident '
             f'<code>{e(orch.get("incident_id", ""))}</code> (<code>{e(orch.get("check_id", ""))}</code>) '
             f'since {e(orch.get("since", ""))} &mdash; a proposal will appear here when ready.</section>']
             if orch.get("state") == "healing" else []),
         *[_ready_card(r, live) for r in d.get("ready", [])],
         '<div class="grid k">' + "".join(
             f'<div class="card kpi"><div class="n {c}">{n}</div><div class="l">{e(l)}</div></div>'
             for (n, l), c in zip(kpis, ["", "critical", "warn", "info", "", "ok"])) + "</div>"]

    # pipelines
    rows = [[p["pipeline_id"], f"v{p['version']}", p["destination"],
             p["last_loaded_at"] or _Raw(f'<span class="mute">n/a</span>'),
             "-" if p["rows"] is None else p["rows"], p["note"]] for p in d["pipelines"]]
    s += ['<section class=card><h2>Pipelines</h2>',
          _table(["pipeline", "version", "destination", "last loaded_at", "rows", "note"], rows, {4}),
          "</section>"]

    # incidents
    s += ['<section class=card><h2>Open incidents</h2>',
          f'<p class="sub">{inc["open"]} open of {inc["total"]} recorded'
          + (f" &middot; business dates {e(inc['date_range'][0])} &rarr; {e(inc['date_range'][1])}"
             if inc["date_range"] else "") + "; other statuses: "
          + (", ".join(f"{e(k)}={v}" for k, v in inc["status_counts"].items() if k != "open") or "none")
          + "</p>", '<div class="grid two"><div><h3>By severity</h3>',
          _bars({k: v for k, v in sev.items()}), "</div><div><h3>By family</h3>",
          _bars(inc["by_family"]), "</div></div>", "<h3>By source</h3>",
          _table(["source", "critical", "warn", "info", "total"],
                 [[r["source_id"], r["critical"], r["warn"], r["info"], r["total"]]
                  for r in inc["by_source"]], {1, 2, 3, 4}),
          "<h3>Most recent open incidents</h3>",
          _table(["date", "severity", "pipeline", "source", "check", "id"],
                 [[r["business_date"], _sev(r["severity"]), r["pipeline_id"], r["source_id"],
                   _Raw(f"<code>{e(r['check_id'])}</code>"), _Raw(f"<code>{e(r['incident_id'])}</code>")]
                  for r in inc["recent_open"]]),
          "<h3>Recently resolved / closed</h3>",
          _table(["date", "status", "pipeline", "check", "id"],
                 [[r["business_date"], _Raw(f'<span class="ok">{e(r["status"])}</span>'), r["pipeline_id"],
                   _Raw(f"<code>{e(r['check_id'])}</code>"), _Raw(f"<code>{e(r['incident_id'])}</code>")]
                  for r in inc.get("recent_closed", [])]), "</section>"]

    # healing
    cls = {"READY_FOR_APPROVAL": "warn", "APPLIED": "ok", "ESCALATED": "info",
           "FAILED": "critical", "REJECTED": "mute"}
    counts = " ".join(_pill(f"{k}: {v}", cls.get(k, "")) for k, v in sorted(d["proposal_counts"].items()))
    s += ['<section class=card><h2>Healing proposals</h2>', f"<p>{counts or 'no proposals'}</p>",
          _table(["proposal", "status", "incident status", "pipeline", "check", "decision", "updated", "note"],
                 [[_Raw(f"<code>{e(r['proposal_id'])}</code>"),
                   _Raw(f'<span class="{cls.get(r["status"], "")}">{e(r["status"])}</span>'),
                   r.get("incident_status") or "-", r["pipeline_id"], _Raw(f"<code>{e(r['check_id'])}</code>"), r["decision"] or "-",
                   r["updated_at"], r["note"]] for r in props]), "</section>"]

    # evaluation
    ev = d["evaluation"]
    s.append('<section class=card><h2>Latest evaluation</h2>')
    s.append(f'<div class="md">{markdown_to_html(ev["markdown"])}</div>' if ev
             else '<p class="mute">no evaluation/results/summary.md yet &mdash; run '
                  '<code>python -m evaluation report</code></p>')
    s.append("</section></main></body></html>")
    return "\n".join(s)
