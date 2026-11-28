"""Overview page (/): proposals awaiting approval first, then stat tiles, pipelines, recent
incidents and proposals, and the latest evaluation.

Static (`build`): no scripts, no forms. Live (`serve`, live=True): a two-step approve control
and a reject form on READY_FOR_APPROVAL cards, plus a small script that refreshes the page
content in place every 5 s -- never while a form field is focused or holds text, or while a
<details> (e.g. the approve confirmation) is open; scroll position is kept.
"""
from __future__ import annotations

import datetime as dt
from html import escape as e

from .governance import audit_table
from .layout import (Links, Raw, a, code, diff_files, diff_html, fmt_int, fmt_ts, fmt_utc, heading,  # noqa: F401
                     markdown_to_html, page, sev, status_class, table, tag)

LIVE_REGION = "live-region"

REFRESH_JS = """<script>(function(){
var ID='live-region',EVERY=5000;
function busy(){
 if(document.hidden)return true;
 var f=document.activeElement;
 if(f&&/^(INPUT|TEXTAREA|SELECT)$/.test(f.tagName))return true;
 var t=document.querySelectorAll('textarea,input[type=text],input[type=search],input:not([type])');
 for(var i=0;i<t.length;i++)if(t[i].value)return true;
 return !!document.querySelector('#'+ID+' details[open]');
}
function later(){setTimeout(tick,EVERY)}
function tick(){
 if(busy())return later();
 fetch(location.href,{cache:'no-store'}).then(function(r){return r.ok?r.text():null}).then(function(html){
  if(!html||busy())return;
  var doc=new DOMParser().parseFromString(html,'text/html');
  var fresh=doc.getElementById(ID),cur=document.getElementById(ID);
  if(fresh&&cur&&fresh.innerHTML!==cur.innerHTML){var x=window.scrollX,y=window.scrollY;cur.innerHTML=fresh.innerHTML;window.scrollTo(x,y)}
  var g=doc.getElementById('generated-at'),gc=document.getElementById('generated-at');
  if(g&&gc)gc.textContent=g.textContent;
 }).catch(function(){}).then(later);
}
later();
})();</script>"""


def _ready_card(r: dict, live: bool, generating: dict | None = None) -> str:
    pid = e(r["proposal_id"])
    files = diff_files(r["diff"])
    body = [f'<section class="card ready"><h2>Awaiting your approval &mdash; <code>{pid}</code></h2>',
            f'<p class="sub">{e(r["pipeline_id"])} &middot; <code>{e(r["check_id"])}</code> &middot; '
            f'incident <code>{e(r["incident_id"])}</code> &middot; agent decision: {e(r["decision"] or "-")}</p>',
            '<h3>Diagnosis</h3>',
            f'<div class="md">{markdown_to_html(r["diagnosis"]) or "<p class=empty>No diagnosis recorded.</p>"}</div>',
            '<h3>Proposed change</h3>', diff_html(r["diff"]) if r["diff"] else '<p class="empty">No diff.</p>',
            '<h3>Validation (before vs after)</h3>',
            f'<div class="md">{markdown_to_html(r["validation"]) or "<p class=empty>No validation recorded.</p>"}</div>']
    if live:
        changes = (", ".join(f"<code>{e(f)}</code>" for f in files) if files
                   else "the staged files of this proposal")
        if generating:      # a failed generation job restores dags/, which would undo this fix
            approve = (f'<p class="warn approve-locked">A pipeline is being generated '
                       f'(<a href="/jobs/{e(generating["job_id"])}">{e(generating["pipeline_id"])}</a>); '
                       'approve after it finishes. Reject is still available.</p>')
        else:
            approve = (
                '<details class="confirm"><summary class="btn approve">Approve &amp; apply&hellip;</summary>'
                '<div class="confirm-box">'
                f'<p><b>This changes production.</b> Files overwritten with the staged version: {changes}.</p>'
                f'<p>Incident <code>{e(r["incident_id"])}</code> moves to <code>resolved_pending_verification</code>; '
                'the decision is logged with your name, channel <code>dashboard</code> and the time.</p>'
                f'<form method="post" action="/approve"><input type="hidden" name="proposal_id" value="{pid}">'
                '<input type="hidden" name="confirm" value="apply">'
                '<button class="approve" type="submit">Confirm apply to production</button></form>'
                '</div></details>')
        body.append(
            '<div class="actions">' + approve +
            '<form method="post" action="/reject">'
            f'<input type="hidden" name="proposal_id" value="{pid}">'
            '<textarea name="reason" placeholder="Reason for rejecting (required)" required></textarea>'
            '<button class="reject" type="submit">Reject</button></form></div>')
    else:
        body.append(f'<p class="muted">Approve: <code>python -m healing apply {pid}</code> &middot; '
                    'serve the live dashboard for buttons.</p>')
    return "".join(body) + "</section>"


def pipelines_table(pipelines: list[dict], open_by_pipeline: dict, links: Links) -> str:
    rows = [[a(links.pipeline(p["pipeline_id"]), p["pipeline_id"]), f"v{p['version']}", code(p["destination"]),
             Raw(f'<span class="nowrap">{e(fmt_utc(p["last_loaded_at"]))}</span>') if p["last_loaded_at"] else Raw('<span class="muted">n/a</span>'),
             fmt_int(p["rows"]), open_by_pipeline.get(p["pipeline_id"], 0),
             Raw(f'<span class="muted">{e(p["note"])}</span>')] for p in pipelines]
    return table(["pipeline", "version", "destination", "last load", "rows", "open incidents", "note"], rows,
                 {4, 5}, empty="No manifests found.")


def incident_rows(records: list[dict], links: Links, with_pipeline: bool = True) -> list[list]:
    return [[r["business_date"], sev(r["severity"]),
             *([a(links.pipeline(r["pipeline_id"]), r["pipeline_id"])] if with_pipeline else []),
             r["source_id"], code(r["check_id"]), r.get("check_family", ""), code(r["incident_id"])]
            for r in records]


INCIDENT_HEADERS = ["date", "severity", "pipeline", "source", "check", "family", "id"]


def render(d: dict, now: dt.datetime | None = None, live: bool = False, flash: str = "",
           flash_error: bool = False, generating: dict | None = None) -> str:
    links = Links(live, {p["pipeline_id"] for p in d["pipelines"]})
    inc = d["incidents"]
    sev_counts = inc["by_severity"]
    orch = d.get("orchestrator") or {}
    pending = d["proposal_counts"].get("READY_FOR_APPROVAL", 0)
    s = []
    if flash:
        s.append(f'<section class="card notice{" err" if flash_error else ""}">{e(flash)}</section>')
    if orch.get("state") == "healing":
        s.append(f'<section class="card notice warn">Orchestrator is healing incident '
                 f'<code>{e(orch.get("incident_id", ""))}</code> (<code>{e(orch.get("check_id", ""))}</code>) '
                 f'since {e(fmt_ts(orch.get("since", "")))} &mdash; a proposal will appear here when ready.</section>')
    ready = d.get("ready", [])
    s += [_ready_card(r, live, generating) for r in ready]
    if not ready:
        s.append('<section class="card"><p class="empty">No proposals awaiting approval.</p></section>')

    tiles = [(inc["open"], "open incidents", "", links.incidents()),
             (sev_counts["critical"], "critical", "critical", links.incidents(severity="critical")),
             (sev_counts["warn"], "warn", "warn", links.incidents(severity="warn")),
             (sev_counts["info"], "info", "info", links.incidents(severity="info")),
             (len(d["pipelines"]), "pipelines", "", links.pipelines()),
             (pending, "proposals awaiting approval", "warn" if pending else "",
              links.proposals("READY_FOR_APPROVAL"))]
    s.append('<div class="grid tiles">' + "".join(
        f'<a class="tile" href="{e(href)}"><div class="n {c}">{n}</div><div class="l">{e(label)}</div></a>'
        for n, label, c, href in tiles) + "</div>")

    s += [f'<section class=card><h2>Pipelines <a class="more" href="{e(links.pipelines())}">all pipelines &rarr;</a></h2>',
          pipelines_table(d["pipelines"], inc.get("by_pipeline", {}), links), "</section>"]

    other = ", ".join(f"{e(k)}={v}" for k, v in inc["status_counts"].items() if k != "open") or "none"
    s += [f'<section class=card><h2>Open incidents <a class="more" href="{e(links.incidents())}">all incidents &rarr;</a></h2>',
          f'<p class="sub">{inc["open"]} open of {inc["total"]} recorded'
          + (f" &middot; business dates {e(inc['date_range'][0])} &rarr; {e(inc['date_range'][1])}"
             if inc["date_range"] else "") + f"; other statuses: {other}</p>",
          "<h3>Most recent open</h3>",
          table(INCIDENT_HEADERS, incident_rows(inc["recent_open"][:8], links), empty="No open incidents."),
          "<h3>Recently resolved / closed</h3>",
          table(["date", "status", "pipeline", "check", "id"],
                [[r["business_date"], tag(r["status"]), a(links.pipeline(r["pipeline_id"]), r["pipeline_id"]),
                  code(r["check_id"]), code(r["incident_id"])] for r in inc.get("recent_closed", [])[:5]],
                empty="Nothing resolved yet."),
          "</section>"]

    counts = " ".join(f'<a href="{e(links.proposals(k))}">{tag(f"{k}: {v}", status_class(k))}</a>'
                      for k, v in sorted(d["proposal_counts"].items()))
    s += [f'<section class=card><h2>Healing proposals <a class="more" href="{e(links.proposals())}">all proposals &rarr;</a></h2>',
          f'<p class="chips">{counts}</p>' if counts else "",
          audit_table(d.get("audit", [])[:8], links, empty="No proposals yet."), "</section>"]

    ev = d["evaluation"]
    s.append('<section class=card><h2>Latest evaluation</h2>')
    s.append(f'<div class="md">{markdown_to_html(ev["markdown"])}</div>' if ev
             else '<p class="empty">Nothing to show: no evaluation/results/summary.md yet &mdash; run '
                  '<code>python -m evaluation report</code>.</p>')
    s.append("</section>")

    body = heading("Overview", "What needs a decision now, then the state of every pipeline.") \
        + f'<div id="{LIVE_REGION}">' + "\n".join(s) + "</div>"
    return page("Pipeline Agent", "overview", body, links, now, head_extra=REFRESH_JS if live else "",
                foot_note="content updates every 5 s, paused while you type" if live else "")

