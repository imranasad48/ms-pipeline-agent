"""Governance page: data contracts, autonomy policy, and the proposal audit trail.

Claim made checkable here: "nothing touches production without a logged human decision."
Everything is read from existing state (manifests/*.yaml, healing/proposals/**), the autonomy
policy is imported from the orchestrator's own constants, and no LLM is involved.
"""
from __future__ import annotations

import datetime as dt
import json
from html import escape as e
from pathlib import Path

from healing.incidents import SKIP_PREFIX
from orchestrator.watcher import ALL_PROPOSAL_STATUSES, is_eligible, load_config

from .data import (NOT_RECORDED, Paths, load_audit, read_manifest, load_history, load_proposal_detail,  # noqa: F401
                   provenance, summarize_audit)
from .layout import (Links, Raw, a, code, diff_html, fmt_ts, heading, markdown_to_html, page, static_name,
                     table, tag)

# ------------------------------------------------------------------ data
def load_contracts(manifests_dir: Path) -> list[dict]:
    """The manifests as written: only fields the manifest actually has are returned."""
    out = []
    for p in sorted(Path(manifests_dir).glob("*.yaml")):
        m = read_manifest(p)
        if m is None:
            continue
        out.append({"pipeline_id": m.get("pipeline_id", p.stem),
                    "version": m.get("version", m.get("manifest_version")),
                    "brief": m.get("brief", ""), "destination": m.get("destination") or {},
                    "sources": m.get("sources") or [], "steps": m.get("steps") or [],
                    "checks": m.get("validation_checks") or [], "file": p.name})
    return out


def autonomy_policy() -> dict:
    """Allowlist and the never-auto-healed rules, taken from the orchestrator's own code."""
    prefixes = tuple(load_config()["auto_heal_prefixes"])
    probe = lambda cid: is_eligible({"check_id": cid, "severity": "critical", "status": "open"}, prefixes)  # noqa: E731
    return {"allow": list(prefixes), "skip_prefix": SKIP_PREFIX,
            "run_health_auto": probe(SKIP_PREFIX + "x"),
            "freshness_auto": probe("freshness.late_file"),
            "statuses": list(ALL_PROPOSAL_STATUSES)}


def assemble_governance(paths: Paths | None = None) -> dict:
    paths = paths or Paths()
    audit = load_audit(paths.proposals)
    for r in audit:
        r["detail"] = load_proposal_detail(paths.proposals, r["proposal_id"])
    return {"provenance": provenance(load_history(paths.history)),
            "contracts": load_contracts(paths.manifests), "autonomy": autonomy_policy(),
            "audit": audit, "audit_counts": summarize_audit(audit)}


# ---------------------------------------------------------------- render
def _contract_html(c: dict, open_: bool = False) -> str:
    dest = c["destination"]
    s = [f'<div id="contract-{e(static_name(c["pipeline_id"]))}">'
         f'<details class="card"{" open" if open_ else ""}><summary>{e(c["pipeline_id"])} &middot; '
         f'version {e(str(c["version"]))} &middot; &rarr; <code>{e(str(dest.get("table", "-")))}</code></summary>']
    if c["brief"]:
        s.append(f'<p class="sub">{e(c["brief"])}</p>')
    if dest:
        s.append("<h3>Destination</h3>" + table(["field", "value"], [[k, code(v)] for k, v in dest.items()]))
    s.append("<h3>Sources</h3>" + table(
        ["id", "format", "path pattern"],
        [[code(x.get("id", "")), x.get("format", ""), code(x.get("path_pattern", ""))] for x in c["sources"]],
        empty="No sources declared."))
    for src in c["sources"]:
        sch, obs = src.get("expected_schema"), src.get("observed")
        if sch:
            nulls = (obs or {}).get("null_rates") or {}
            s.append(f'<h3>Schema &mdash; <code>{e(str(src.get("id", "")))}</code></h3>' + table(
                ["column", "type"] + (["baseline null rate"] if nulls else []),
                [[code(col), typ] + ([nulls.get(col, "")] if nulls else []) for col, typ in sch.items()]))
        if obs:
            s.append(f'<h3>Baseline &mdash; <code>{e(str(src.get("id", "")))}</code></h3>' + table(
                ["field", "observed"],
                [[k, v] for k, v in obs.items() if k != "null_rates"]))
    if c["steps"]:
        s.append("<h3>Steps</h3>" + table(["step", "function", "description"],
                 [[x.get("id", ""), code(x.get("function", "")), x.get("description", "")] for x in c["steps"]]))
    s.append("<h3>Validation checks</h3>" + table(
        ["id", "type", "rule", "params"],
        [[code(x.get("id", "")), x.get("type", ""), x.get("description", ""),
          code(json.dumps(x.get("params", {}), default=str))] for x in c["checks"]],
        empty="No validation checks declared."))
    return "".join(s) + "</details></div>"


def _autonomy_html(p: dict) -> str:
    allow = ", ".join(f"<code>{e(x)}*</code>" for x in p["allow"])
    return ('<section class=card><h2>Autonomy policy</h2>'
            '<h3>What the system may do alone</h3>'
            f'<p>Start a heal for an open warn/critical incident whose check id begins with: {allow}.</p>'
            '<p class="muted">The agent only writes a <i>staged proposal</i>; nothing is applied.</p>'
            '<h3>What always needs a human</h3>'
            '<ul class="rules">'
            '<li>Every production change requires a human <b>Approve</b> (dashboard button or <code>healing apply</code>); '
            'it is logged with who, how and when.</li>'
            f'<li>Any check not on the allowlist is escalated, never auto-healed &mdash; '
            f'including <code>freshness.*</code> (auto-heal: {"yes" if p["freshness_auto"] else "no"}).</li>'
            f'<li><code>{e(p["skip_prefix"])}*</code> incidents are never auto-healed '
            f'(auto-heal: {"yes" if p["run_health_auto"] else "no"}).</li>'
            '<li>Domain events are escalated, never &ldquo;fixed&rdquo;.</li>'
            '<li>Detection is deterministic: no LLM. The agent only writes staged proposals.</li></ul>'
            '<h3>Validation gate</h3>'
            '<p>READY only if the target check passes after the patch, there are zero regressions and no newly '
            'surfaced failures, and any still-failing check is excused only when it also fails in the control run.</p>'
            f'<p class="muted">Proposal statuses the orchestrator will not re-heal: '
            f'{", ".join(f"<code>{e(x)}</code>" for x in p["statuses"])}.</p></section>')


_short_ts = fmt_ts          # kept for callers of the old name


def audit_stats(counts: dict) -> str:
    return "<p>" + "".join(
        f'<span class="stat"><b>{counts[k]}</b> <span class="muted">{e(label)}</span></span>'
        for k, label in [("total", "total"), ("applied", "applied"), ("rejected", "rejected"),
                         ("failed_validation", "failed validation"), ("escalated", "escalated"),
                         ("awaiting", "awaiting approval")]) + "</p>"


def audit_table(rows: list[dict], links: Links, empty: str = "No proposals yet.") -> str:
    """The audit-trail table; the single renderer for proposal rows (Governance, Proposals,
    per-pipeline history, Overview)."""
    out = []
    for r in rows:
        d = r.get("detail") or {"diagnosis": "", "diff": ""}
        status = tag(r["status"])
        if r["verdict"] != r["status"]:          # only say it twice when the two differ
            status += f'<br><span class="muted">validated: {e(r["verdict"])}</span>'
        if r["decided_by"] != NOT_RECORDED:
            who = e(r["decided_by"] + (f" ({r['decided_via']})" if r["decided_via"] else ""))
        else:
            who = NOT_RECORDED if r["human_decision"] else "&mdash;"
        when = e(fmt_ts(r["decided_at"])) if r["human_decision"] else "&mdash;"
        more = (f'<details><summary>details</summary><p>incident <code>{e(r["incident_id"])}</code> &middot; '
                f'check <code>{e(r["check_id"] or "-")}</code></p>'
                + (f'<div class="md">{markdown_to_html(d["diagnosis"])}</div>' if d["diagnosis"] else "")
                + (diff_html(d["diff"]) if d["diff"] else "") + "</details>")
        pipe = r["pipeline_id"]
        out.append([
            code(r["proposal_id"], "pid"), Raw(f'<span class="nowrap">{e(fmt_ts(r["created_at"]))}</span>'),
            a(links.pipeline(pipe) if pipe != NOT_RECORDED else None, pipe),
            Raw(f'<code title="incident {e(r["incident_id"])}">{e(r["check_kind"])}</code>'),
            r["decision"], Raw(status), Raw(who), Raw(f'<span class="nowrap">{when}</span>'),
            Raw((e(r["reason"]) if r["reason"] else "&mdash;") + more)])
    return table(["proposal", "created", "pipeline", "check kind", "agent", "status",
                  "decided by", "decided at", "reason"], out, empty=empty, cls="audit")


def _audit_html(rows: list[dict], counts: dict, links: Links) -> str:
    return ('<section class="card audit"><h2>Audit trail</h2>'
            '<p class="sub">Every proposal ever created, newest first (index.json + proposal directories, '
            'including <code>_archive/</code>). &ldquo;not recorded&rdquo; = the decision predates actor logging; '
            'nothing is backfilled.</p>' + audit_stats(counts) + audit_table(rows, links) + "</section>")


def provenance_html(rec: dict | None, links: Links) -> str:
    """'created by ... on ... from prompt ...' for a generated pipeline, else 'not recorded'."""
    if not rec:
        return ('<p class="muted">Provenance: not recorded (built before generation history was kept '
                'in <code>agent/generation_history.jsonl</code>).</p>')
    job = (f' &middot; job <a href="/jobs/{e(rec["job_id"])}"><code>{e(rec["job_id"])}</code></a>'
           if links.live and rec.get("job_id") else "")
    rows = "" if rec.get("rows_loaded") is None else f' &middot; {rec["rows_loaded"]:,} rows loaded at creation'
    return (f'<p>Created by <b>{e(str(rec.get("actor", "?")))}</b> via {e(str(rec.get("channel", "?")))} on '
            f'<span class="nowrap">{e(fmt_ts(rec.get("finished_at") or rec.get("started_at") or ""))}</span>'
            f' from sources {", ".join(f"<code>{e(str(x))}</code>" for x in rec.get("sources") or [])}{rows}{job}.</p>'
            f'<p class="muted">Prompt:</p><blockquote class="prompt">{e(str(rec.get("prompt", "")))}</blockquote>')


def _provenance_section(contracts: list[dict], prov: dict, links: Links) -> str:
    rows = []
    for c in contracts:
        r = prov.get(c["pipeline_id"])
        if r:
            what = Raw(f'{e(str(r.get("actor", "?")))} via {e(str(r.get("channel", "?")))}')
            when = fmt_ts(r.get("finished_at") or "")
            prompt = str(r.get("prompt", ""))
            prompt = prompt if len(prompt) <= 140 else prompt[:140] + "…"
        else:
            what, when, prompt = Raw('<span class="muted">not recorded</span>'), "", ""
        rows.append([a(links.pipeline(c["pipeline_id"]), c["pipeline_id"]), what, when, prompt])
    return ('<section class=card><h2>Pipeline provenance</h2>'
            '<p class="sub">Who created each pipeline, when, and from which prompt '
            '(<code>agent/generation_history.jsonl</code>, append-only).</p>'
            + table(["pipeline", "created by", "created", "prompt"], rows, empty="No manifests found.")
            + "</section>")


def render_governance(g: dict, now: dt.datetime | None = None, live: bool = False, open_pid: str = "") -> str:
    links = Links(live, {c["pipeline_id"] for c in g["contracts"]})
    body = [heading("Governance", "Claim: <b>nothing touches production without a logged human decision.</b> "
                    "No auto-refresh on this page."),
            '<section class=card><h2>Data contracts</h2>'
            '<p class="sub">Rendered from <code>manifests/*.yaml</code>; fields a manifest lacks are omitted.</p>',
            "".join(_contract_html(c, c["pipeline_id"] == open_pid) for c in g["contracts"])
            or '<p class="empty">No manifests found.</p>',
            "</section>", _provenance_section(g["contracts"], g.get("provenance", {}), links),
            _autonomy_html(g["autonomy"]), _audit_html(g["audit"], g["audit_counts"], links)]
    return page("Governance", "governance", "".join(body), links, now)
