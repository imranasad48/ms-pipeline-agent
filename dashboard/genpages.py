"""Live-only pages for generating a pipeline: /new (source catalog + form), /jobs, /jobs/<id>.

The job page streams the agent's output by polling /jobs/<id>/poll every 2 s (fetch, no reload);
the log stays pinned to the bottom unless the user has scrolled up. No browser dialogs.
"""
from __future__ import annotations

import datetime as dt
import json
import re
from html import escape as e

from .catalog import build_catalog
from .jobs import ACTIVE, PROMPT_MAX, JobManager
from .layout import Links, Raw, a, fmt_ts, heading, page, table, tag

NEW_JS = """<script>(function(){
var f=document.getElementById('gen-form');if(!f)return;
var id=f.querySelector('input[name=pipeline_id]'),note=document.getElementById('covered-note'),edited=!!id.value;
id.addEventListener('input',function(){edited=!!id.value});
f.addEventListener('change',function(ev){
 var boxes=[].slice.call(f.querySelectorAll('input[name=source]:checked'));
 if(!edited){id.value=boxes.length?boxes[0].getAttribute('data-suggest'):''}
 var cov=boxes.filter(function(b){return b.getAttribute('data-covered')}).map(function(b){return b.value+' (used by '+b.getAttribute('data-covered')+')'});
 note.hidden=!cov.length;note.querySelector('span').textContent=cov.join('; ');
});
f.addEventListener('submit',function(){var b=f.querySelector('button[type=submit]');b.disabled=true;b.textContent='Starting\\u2026'});
})();</script>"""

JOB_JS = """<script>(function(){
var box=document.getElementById('job'),log=document.getElementById('log');if(!box)return;
var url=box.getAttribute('data-poll'),off=+box.getAttribute('data-offset'),done=box.getAttribute('data-terminal')==='1';
function pinned(){return log.scrollHeight-log.scrollTop-log.clientHeight<24}
log.scrollTop=log.scrollHeight;
function tick(){
 if(done)return;
 fetch(url+'?offset='+off,{cache:'no-store'}).then(function(r){return r.ok?r.json():null}).then(function(j){
  if(!j)return;
  var stick=pinned();
  if(j.log){log.appendChild(document.createTextNode(j.log))}
  off=j.offset;
  if(stick)log.scrollTop=log.scrollHeight;
  var head=document.getElementById('job-head');
  if(head&&j.head_html!==head.innerHTML)head.innerHTML=j.head_html;
  done=j.terminal;
 }).catch(function(){}).then(function(){if(!done)setTimeout(tick,2000)});
}
setTimeout(tick,2000);
})();</script>"""


def _elapsed(job: dict, now: dt.datetime | None = None) -> str:
    try:
        start = dt.datetime.fromisoformat(job.get("started_at") or job["created_at"])
    except (TypeError, ValueError, KeyError):
        return "-"
    end = dt.datetime.fromisoformat(job["finished_at"]) if job.get("finished_at") else (now or dt.datetime.now())
    s = max(0, int((end - start).total_seconds()))
    return f"{s // 3600}h {s // 60 % 60:02d}m {s % 60:02d}s" if s >= 3600 else f"{s // 60}m {s % 60:02d}s"


def _d(x: str) -> str:
    """File-name date parts: '2026-08-14T05' -> '2026-08-14 05:00'; dates and weeks unchanged."""
    m = re.match(r"^(\d{4}-\d\d-\d\d)T(\d\d):?(\d\d)?$", x)
    return f"{m.group(1)} {m.group(2)}:{m.group(3) or '00'}" if m else x


def _source_rows(catalog: list[dict], selected: set, links: Links) -> list[list]:
    rows = []
    for s in catalog:
        box = Raw(f'<input type="checkbox" name="source" value="{e(s["key"])}" data-suggest="{e(s["suggest"])}"'
                  f'{" data-covered=" + chr(34) + e(", ".join(s["covered_by"])) + chr(34) if s["covered_by"] else ""}'
                  f'{" checked" if s["key"] in selected else ""} aria-label="{e(s["key"])}">')
        if s["kind"] == "table":
            what, size, rng = "SQLite table", f'{s["columns"]} columns', ""
        else:
            what = "files"
            size = f'{s["count"]:,} file' + ("s" if s["count"] != 1 else "")
            rng = (f'{_d(s["date_range"][0])} → {_d(s["date_range"][1])}' if s.get("date_range") else "")
        cov = (Raw(", ".join(a(links.pipeline(p), p) for p in s["covered_by"]))
               if s["covered_by"] else tag("no pipeline yet", "ok"))
        rows.append([box, Raw(f'<label><code>{e(s["key"])}</code></label>'), what, size, rng, cov])
    return rows


def render_new(jobs: JobManager, manifests_dir, pipelines: set, form: dict | None = None,
               errors: list | None = None, now=None) -> str:
    links = Links(True, pipelines)
    cfg = jobs.cfg
    catalog = build_catalog(cfg.data_root, manifests_dir)
    form = form or {}
    selected = set(form.get("source") or [])
    pid = (form.get("pipeline_id") or [""])[0]
    prompt = (form.get("prompt") or [""])[0]
    active = jobs.active()
    s = [heading("New pipeline", f"Pick sources under the data root <code>{e(str(cfg.data_root))}</code>, "
                                 "name the pipeline and describe what it should produce. The generation agent "
                                 "builds it, then the dashboard switches its Airflow DAG on and triggers the first run; Airflow "
                                 "runs it daily from then on.")]
    if active:
        s.append(f'<section class="card notice warn">A generation job is running for '
                 f'<b>{e(active["pipeline_id"])}</b> &mdash; <a href="/jobs/{e(active["job_id"])}">watch it</a>. '
                 'Only one job runs at a time.</section>')
    if errors:
        s.append('<section class="card notice err"><b>Not started:</b><ul>'
                 + "".join(f"<li>{e(x)}</li>" for x in errors) + "</ul></section>")
    unc = [x for x in catalog if not x["covered_by"]]
    cov = [x for x in catalog if x["covered_by"]]
    hdr = ["", "source", "kind", "size", "dates", "pipelines"]
    s += ['<form id="gen-form" method="post" action="/generate">',
          f'<section class="card"><h2>1. Sources <span class="more muted">{len(unc)} without a pipeline, '
          f'{len(cov)} already used</span></h2>',
          "<h3>No pipeline yet</h3>",
          table(hdr, _source_rows(unc, selected, links), empty="Every source already has a pipeline."),
          "<h3>Already used by a pipeline</h3>",
          '<p class="muted">Selecting one of these is allowed: the new pipeline reads the same files as the '
          'existing one, which is left untouched.</p>',
          table(hdr, _source_rows(cov, selected, links), empty="None of the sources is used yet."),
          "" if catalog else f'<p class="empty">No sources found under <code>{e(str(cfg.data_root))}</code>. '
                             'Start the server with <code>HEAL_DATA_ROOT</code> pointing at the data.</p>',
          "</section>",
          '<section class="card"><h2>2. Pipeline</h2>',
          '<p id="covered-note" class="warn" hidden>Note: already used by another pipeline: <span></span></p>',
          '<p><label>Pipeline id<br><input type="text" name="pipeline_id" required pattern="[a-z][a-z0-9_]{1,47}" '
          f'maxlength="48" value="{e(pid)}" placeholder="e.g. customers" autocomplete="off"></label><br>'
          '<span class="muted">Lowercase letters, digits and underscores. Becomes '
          '<code>dags/&lt;id&gt;/</code>, <code>dags/&lt;id&gt;_dag.py</code> and '
          '<code>manifests/&lt;id&gt;.yaml</code>. Existing pipelines are never overwritten.</span></p>',
          f'<p><label>What should the pipeline produce?<br><textarea name="prompt" required maxlength="{PROMPT_MAX}" '
          'class="prompt-input" placeholder="e.g. Load the daily customer snapshots into a table named customers, '
          f'one row per customer with the latest values.">{e(prompt)}</textarea></label></p>',
          '<p><button class="btn primary" type="submit"'
          f'{" disabled" if active else ""}>Generate pipeline</button> '
          '<span class="muted">Runs the generation agent on your Claude subscription; usually several minutes. '
          f'Stops automatically after {cfg.time_limit_s / 60:g} minutes.</span></p></section></form>']
    recent = jobs.list(10)
    s.append('<section class="card"><h2>Recent jobs <a class="more" href="/jobs">all jobs &rarr;</a></h2>'
             + jobs_table(recent, links) + "</section>")
    return page("New pipeline", "new", "".join(s), links, now, body_end=NEW_JS)


def jobs_table(jobs: list[dict], links: Links, now=None) -> str:
    return table(["job", "pipeline", "status", "started", "elapsed", "by", "rows"],
                 [[a(f"/jobs/{j['job_id']}", Raw(f"<code>{e(j['job_id'])}</code>")),
                   a(links.pipeline(j["pipeline_id"]) if j["status"] == "succeeded" else None, j["pipeline_id"]),
                   tag(j["status"]), fmt_ts(j.get("started_at") or j.get("created_at")), _elapsed(j, now),
                   j.get("actor", ""), "-" if j.get("rows_loaded") is None else f'{j["rows_loaded"]:,}']
                  for j in jobs], {6}, empty="No generation jobs yet.")


def render_jobs(jobs: JobManager, pipelines: set, now=None) -> str:
    links = Links(True, pipelines)
    body = (heading("Generation jobs", 'Newest first. <a href="/new">Start a new pipeline</a>.')
            + '<section class="card">' + jobs_table(jobs.list(200), links, now) + "</section>")
    return page("Jobs", "new", body, links, now)


def job_head(job: dict, pipelines: set, now=None) -> str:
    """The part of the job page that changes while it runs (also sent with each poll)."""
    links = Links(True, pipelines | ({job["pipeline_id"]} if job["status"] == "succeeded" else set()))
    st = job["status"]
    gen = job.get("kind", "generate") == "generate"
    out = []
    if st in ("failed", "interrupted", "cancelled") and job.get("error"):
        undo = []
        if job.get("airflow_state") is not None or job.get("airflow_schedule") is not None:
            undo.append(f'the Airflow DAG <code>{e(job["pipeline_id"])}</code> is paused')
        if gen:
            undo.append("dags/ and manifests/ were restored to how they were before this job"
                        + (f'; {len(job["quarantined"])} file(s) it wrote were moved to '
                           f'<code>dashboard/jobs/{e(job["job_id"])}/quarantine/</code>' if job.get("quarantined") else ""))
        out.append(f'<section class="card notice {"err" if st == "failed" else "warn"}"><b>'
                   f'{"Failed" if st == "failed" else st.capitalize()}:</b> {e(job["error"])}'
                   + (f'<br><span class=muted>{"; ".join(undo)}.</span>' if undo else "") + "</section>")
    if job.get("airflow_log_tail") and st != "succeeded":
        task = job.get("airflow_failed_task")
        out.append('<section class="card"><h2>Airflow ' + (f'task <code>{e(task)}</code> ' if task else "")
                   + 'log (tail)</h2>' + f'<pre class="log small">{e(job["airflow_log_tail"])}</pre></section>')
    if st == "succeeded":
        tbl = e(job.get("destination_table") or "")
        what = (f'<b>Pipeline {e(job["pipeline_id"])} is ready.</b> ' if gen
                else f'<b>DAG {e(job["pipeline_id"])} is enabled.</b> ')
        out.append(f'<section class="card notice">{what}'
                   f'Airflow run <code>{e(job.get("airflow_run_id") or "")}</code> succeeded and the table '
                   f'<code>{tbl}</code> now has {job.get("rows_loaded") or 0:,} rows. '
                   f'<a href="{e(links.pipeline(job["pipeline_id"]) or "#")}">Open the pipeline page &rarr;</a><br>'
                   f'<span class="muted">The DAG is switched on and Airflow runs it on its schedule from now on '
                   f'({e(job.get("airflow_schedule") or "daily")}, catchup off). Detection was not run.</span></section>')
    for w in job.get("warnings") or []:
        out.append(f'<section class="card notice warn">{e(w)}</section>')
    steps = (["queued", "generating", "orchestrating", "succeeded"] if gen
             else ["queued", "orchestrating", "succeeded"])
    prog = " &rarr; ".join(f'<b>{x}</b>' if x == st else f'<span class="muted">{x}</span>' for x in steps)
    cancel = (f'<form method="post" action="/jobs/{e(job["job_id"])}/cancel" class="inline">'
              '<button class="reject" type="submit">Cancel job</button></form>') if st in ACTIVE else ""
    covered = (f'<dt>note</dt><dd class="warn">source(s) already used by: {e(", ".join(job["covered_by"]))}</dd>'
               if job.get("covered_by") else "")
    out.append('<section class="card"><div class="jobbar">'
               f'<div>{tag(st)} <span class="muted">elapsed</span> <b>{_elapsed(job, now)}</b>'
               f'{" &middot; " + prog if st in ACTIVE or st == "succeeded" else ""}</div>{cancel}</div>'
               '<dl class="kv">'
               f'<dt>pipeline id</dt><dd><code>{e(job["pipeline_id"])}</code></dd>'
               f'<dt>sources</dt><dd>{", ".join(f"<code>{e(x)}</code>" for x in job["sources"])}</dd>{covered}'
               f'<dt>started by</dt><dd>{e(job.get("actor", ""))} via dashboard</dd>'
               f'<dt>started</dt><dd class="nowrap">{e(fmt_ts(job.get("started_at") or job.get("created_at")))}</dd>'
               f'<dt>finished</dt><dd class="nowrap">{e(fmt_ts(job.get("finished_at")) or "-")}</dd>'
               + (f'<dt>agent exit code</dt><dd>{e(str(job.get("exit_code") if job.get("exit_code") is not None else "-"))}</dd>'
                  f'<dt>agent data root</dt><dd><code>{e(job.get("data_root", ""))}</code></dd>' if gen else "")
               + f'<dt>Airflow DAG</dt><dd>{_dag_state(job)}</dd>'
               f'<dt>Airflow run</dt><dd><code>{e(job.get("airflow_run_id") or "-")}</code> '
               f'{tag(job["airflow_state"]) if job.get("airflow_state") else "<span class=muted>not started</span>"}</dd>'
               f'<dt>schedule</dt><dd>{e(job.get("airflow_schedule") or "-")}'
               f'{", catchup " + ("on" if job["airflow_catchup"] else "off") if job.get("airflow_catchup") is not None else ""}</dd>'
               f'<dt>Airflow reads</dt><dd><code>{e(job.get("airflow_data") or "-")}</code></dd>'
               f'<dt>warehouse</dt><dd><code>{e(job.get("warehouse", ""))}</code></dd>'
               '</dl></section>')
    return "".join(out)


def _dag_state(job: dict) -> str:
    if job.get("dag_enabled"):
        return tag("enabled", "ok") + " runs on its schedule"
    if job.get("airflow_schedule") is None and job["status"] in ACTIVE:
        return '<span class="muted">not reached yet</span>'
    return tag("paused", "muted") if job.get("airflow_schedule") is not None else '<span class="muted">not touched</span>'


def render_job(jobs: JobManager, job_id: str, pipelines: set, now=None) -> str | None:
    job = jobs.get(job_id)
    if not job:
        return None
    terminal = job["status"] not in ACTIVE
    text, off = jobs.read_log(job_id, 0, 5_000_000, whole_lines=not terminal)
    links = Links(True, pipelines)
    gen = job.get("kind", "generate") == "generate"
    what = (f'Generating <b>{e(job["pipeline_id"])}</b>, then running it through Airflow.' if gen
            else f'Enable schedule and run now: <b>{e(job["pipeline_id"])}</b> through Airflow.')
    body = (heading(f"Job {job_id}", what + ' <a href="/jobs">All jobs</a>')
            + f'<div id="job" data-poll="/jobs/{e(job_id)}/poll" data-offset="{off}" data-terminal="{int(terminal)}">'
            + f'<div id="job-head">{job_head(job, pipelines, now)}</div>'
            + ('<section class="card"><h2>Prompt</h2>'
               + f'<blockquote class="prompt">{e(job["prompt"])}</blockquote>'
               + '<details><summary>full brief sent to the agent</summary>'
               + f'<pre class="log small">{e(job["brief"])}</pre></details></section>' if gen else "")
            + f'<section class="card"><h2>{"Agent and Airflow output" if gen else "Airflow output"}</h2>'
            + f'<pre id="log" class="log">{e(text)}</pre></section></div>')
    return page(f"Job {job['pipeline_id']}", "new", body, links, now, body_end=JOB_JS)


def poll_json(jobs: JobManager, job_id: str, offset: int, pipelines: set) -> str | None:
    job = jobs.get(job_id)
    if not job:
        return None
    terminal = job["status"] not in ACTIVE
    text, off = jobs.read_log(job_id, offset, whole_lines=not terminal)
    if terminal and off < jobs.log_path(job_id).stat().st_size:
        terminal = False                     # more log to send before the client stops polling
    return json.dumps({"status": job["status"], "terminal": terminal, "elapsed": _elapsed(job),
                       "log": text, "offset": off, "head_html": job_head(job, pipelines)})
