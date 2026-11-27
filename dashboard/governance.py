"""Governance page: data contracts, autonomy policy, and the proposal audit trail.

Claim made checkable here: "nothing touches production without a logged human decision."
Everything is read from existing state (manifests/*.yaml, healing/proposals/**), the autonomy
policy is imported from the orchestrator's own constants, and no LLM is involved.
"""
from __future__ import annotations

import datetime as dt
import json
import re
from html import escape as e
from pathlib import Path

import yaml

from healing.incidents import SKIP_PREFIX
from orchestrator.watcher import ALL_PROPOSAL_STATUSES, is_eligible, load_config

from .data import Paths, load_proposal_detail
from .render import CSS, _diff_html, _Raw, _table, markdown_to_html, nav

NOT_RECORDED = "not recorded"
EXTRA_CSS = """
main{max-width:1280px}
details{margin:8px 0}summary{cursor:pointer;font-weight:600}details>summary+*{margin-top:8px}
.audit table{font-size:12px}.audit td,.audit th{padding:4px 6px}.audit code.pid{font-size:11px;white-space:nowrap}
.audit td:last-child{min-width:180px;max-width:320px;overflow-wrap:anywhere}.audit td:nth-child(2),.audit td:nth-child(8){white-space:nowrap}
.rules li{margin:3px 0}.stat{display:inline-block;margin:0 18px 4px 0}.stat b{font-size:18px}
"""


# ------------------------------------------------------------------ data
def load_contracts(manifests_dir: Path) -> list[dict]:
    """The manifests as written: only fields the manifest actually has are returned."""
    out = []
    for p in sorted(Path(manifests_dir).glob("*.yaml")):
        m = yaml.safe_load(p.read_text()) or {}
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


def _read_json(path: Path) -> dict:
    try:
        return json.loads(Path(path).read_text())
    except (OSError, ValueError):
        return {}


def _verdict_of(root: Path | None) -> str:
    if root is None:
        return NOT_RECORDED
    try:
        m = re.search(r"\*\*Verdict: (\S+)\*\*", (root / "proposal.md").read_text())
    except OSError:
        return NOT_RECORDED
    return m.group(1) if m else NOT_RECORDED


def load_audit(index_path: Path) -> list[dict]:
    """Every proposal ever created, newest first: index.json entries plus any proposal
    directory (live or _archive/) the index does not know about. Missing facts stay
    'not recorded' -- nothing is inferred or backfilled."""
    index_path = Path(index_path)
    base = index_path.parent
    idx = _read_json(index_path)
    dirs: dict[str, Path] = {}
    for sub in (base / "_archive", base):          # live dir wins over archive on a clash
        if sub.is_dir():
            for d in sub.iterdir():
                if d.is_dir() and d.name.startswith("p-"):
                    dirs[d.name] = d
    rows = []
    for pid in sorted(set(idx) | set(dirs)):
        v, root = idx.get(pid, {}), dirs.get(pid)
        case = _read_json(root / "case.json") if root else {}
        inc = _read_json(root / "staging/case/incident.json") if root else {}
        rej = _read_json(root / "rejection.json") if root else {}
        dec = _read_json(root / "staging/agent_out/decision.json") if root else {}
        check_id = v.get("check_id") or inc.get("check_id") or ""
        rows.append({
            "proposal_id": pid,
            "created_at": v.get("created_at") or case.get("created_at") or NOT_RECORDED,
            "pipeline_id": v.get("pipeline_id") or case.get("pipeline_id") or NOT_RECORDED,
            "check_id": check_id, "check_kind": check_id.split(":")[0] or NOT_RECORDED,
            "incident_id": v.get("incident_id") or case.get("incident_id") or NOT_RECORDED,
            "decision": v.get("decision") or dec.get("decision") or NOT_RECORDED,
            "verdict": _verdict_of(root),
            "status": v.get("status") or "not in index",
            "decided_by": v.get("decided_by") or rej.get("decided_by") or NOT_RECORDED,
            "decided_via": v.get("decided_via") or rej.get("decided_via") or "",
            "decided_at": v.get("decided_at") or rej.get("decided_at") or rej.get("rejected_at") or NOT_RECORDED,
            "reason": v.get("reject_reason") or rej.get("reason") or "",
            "human_decision": v.get("status") in ("APPLIED", "REJECTED"),
        })
    return sorted(rows, key=lambda r: (r["created_at"] if r["created_at"] != NOT_RECORDED else "", r["proposal_id"]),
                  reverse=True)


def summarize_audit(rows: list[dict]) -> dict:
    n = lambda *st: sum(r["status"] in st for r in rows)  # noqa: E731
    return {"total": len(rows), "applied": n("APPLIED"), "rejected": n("REJECTED"),
            "failed_validation": n("FAILED_VALIDATION", "FAILED"), "escalated": n("ESCALATED"),
            "awaiting": n("READY_FOR_APPROVAL")}


def assemble_governance(paths: Paths | None = None) -> dict:
    paths = paths or Paths()
    audit = load_audit(paths.proposals)
    for r in audit:
        r["detail"] = load_proposal_detail(paths.proposals, r["proposal_id"])
    return {"contracts": load_contracts(paths.manifests), "autonomy": autonomy_policy(),
            "audit": audit, "audit_counts": summarize_audit(audit)}


# ---------------------------------------------------------------- render
def _code(x) -> _Raw:
    return _Raw(f"<code>{e(str(x))}</code>")


def _contract_html(c: dict) -> str:
    dest = c["destination"]
    s = [f'<details class="card"><summary>{e(c["pipeline_id"])} &middot; version {e(str(c["version"]))} '
         f'&middot; &rarr; <code>{e(str(dest.get("table", "-")))}</code></summary>']
    if c["brief"]:
        s.append(f'<p class="sub">{e(c["brief"])}</p>')
    if dest:
        s.append("<h3>Destination</h3>" + _table(["field", "value"], [[k, _code(v)] for k, v in dest.items()]))
    s.append("<h3>Sources</h3>" + _table(
        ["id", "format", "path pattern"],
        [[_code(x.get("id", "")), x.get("format", ""), _code(x.get("path_pattern", ""))] for x in c["sources"]]))
    for src in c["sources"]:
        sch, obs = src.get("expected_schema"), src.get("observed")
        if sch:
            nulls = (obs or {}).get("null_rates") or {}
            s.append(f'<h3>Schema &mdash; <code>{e(str(src.get("id", "")))}</code></h3>' + _table(
                ["column", "type"] + (["baseline null rate"] if nulls else []),
                [[_code(col), typ] + ([nulls.get(col, "")] if nulls else []) for col, typ in sch.items()]))
        if obs:
            s.append(f'<h3>Baseline &mdash; <code>{e(str(src.get("id", "")))}</code></h3>' + _table(
                ["field", "observed"],
                [[k, v] for k, v in obs.items() if k != "null_rates"]))
    if c["steps"]:
        s.append("<h3>Steps</h3>" + _table(["step", "function", "description"],
                 [[x.get("id", ""), _code(x.get("function", "")), x.get("description", "")] for x in c["steps"]]))
    s.append("<h3>Validation checks</h3>" + _table(
        ["id", "type", "rule", "params"],
        [[_code(x.get("id", "")), x.get("type", ""), x.get("description", ""),
          _code(json.dumps(x.get("params", {}), default=str))] for x in c["checks"]]))
    return "".join(s) + "</details>"


def _autonomy_html(a: dict) -> str:
    allow = ", ".join(f"<code>{e(p)}*</code>" for p in a["allow"])
    return ('<section class=card><h2>Autonomy policy</h2>'
            '<h3>What the system may do alone</h3>'
            f'<p>Start a heal for an open warn/critical incident whose check id begins with: {allow}.</p>'
            '<p class="mute">The agent only writes a <i>staged proposal</i>; nothing is applied.</p>'
            '<h3>What always needs a human</h3>'
            '<ul class="rules">'
            '<li>Every production change requires a human <b>Approve</b> (dashboard button or <code>healing apply</code>); '
            'it is logged with who, how and when.</li>'
            f'<li>Any check not on the allowlist is escalated, never auto-healed &mdash; '
            f'including <code>freshness.*</code> (auto-heal: {"yes" if a["freshness_auto"] else "no"}).</li>'
            f'<li><code>{e(a["skip_prefix"])}*</code> incidents are never auto-healed '
            f'(auto-heal: {"yes" if a["run_health_auto"] else "no"}).</li>'
            '<li>Domain events are escalated, never &ldquo;fixed&rdquo;.</li>'
            '<li>Detection is deterministic: no LLM. The agent only writes staged proposals.</li></ul>'
            '<h3>Validation gate</h3>'
            '<p>READY only if the target check passes after the patch, there are zero regressions and no newly '
            'surfaced failures, and any still-failing check is excused only when it also fails in the control run.</p>'
            f'<p class="mute">Proposal statuses the orchestrator will not re-heal: '
            f'{", ".join(f"<code>{e(s)}</code>" for s in a["statuses"])}.</p></section>')


def _short_ts(ts: str) -> str:
    """'2026-11-28T01:12:43' -> '2026-11-28 01:12'; anything else is shown unchanged."""
    return ts[:16].replace("T", " ") if re.match(r"\d{4}-\d\d-\d\dT\d\d:\d\d", ts or "") else ts


def _audit_html(rows: list[dict], counts: dict) -> str:
    stats = "".join(f'<span class="stat"><b>{counts[k]}</b> <span class="mute">{e(label)}</span></span>'
                    for k, label in [("total", "total"), ("applied", "applied"), ("rejected", "rejected"),
                                     ("failed_validation", "failed validation"), ("escalated", "escalated"),
                                     ("awaiting", "awaiting approval")])
    cls = {"APPLIED": "ok", "READY_FOR_APPROVAL": "warn", "ESCALATED": "info", "REJECTED": "mute",
           "FAILED": "critical", "FAILED_VALIDATION": "critical"}
    table_rows = []
    for r in rows:
        d = r["detail"]
        status = f'<span class="{cls.get(r["status"], "")}">{e(r["status"])}</span>'
        if r["verdict"] != r["status"]:          # only say it twice when the two differ
            status += f'<br><span class="mute">validated: {e(r["verdict"])}</span>'
        who = (r["decided_by"] + (f" ({r['decided_via']})" if r["decided_via"] else "")
               if r["decided_by"] != NOT_RECORDED else NOT_RECORDED) if r["human_decision"] or r["decided_by"] != NOT_RECORDED \
            else "&mdash;"
        when = _short_ts(r["decided_at"]) if r["human_decision"] else "&mdash;"
        more = (f'<details><summary>details</summary><p>incident <code>{e(r["incident_id"])}</code> &middot; '
                f'check <code>{e(r["check_id"] or "-")}</code></p>'
                + (f'<div class="md">{markdown_to_html(d["diagnosis"])}</div>' if d["diagnosis"] else "")
                + (_diff_html(d["diff"]) if d["diff"] else "") + "</details>")
        table_rows.append([
            _Raw(f'<code class="pid">{e(r["proposal_id"])}</code>'), _short_ts(r["created_at"]),
            r["pipeline_id"], _Raw(f'<code title="incident {e(r["incident_id"])}">{e(r["check_kind"])}</code>'),
            r["decision"], _Raw(status),
            _Raw(who if who == "&mdash;" else e(who)), _Raw(when if when == "&mdash;" else e(when)),
            _Raw((e(r["reason"]) if r["reason"] else "&mdash;") + more)])
    return ('<section class="card audit"><h2>Audit trail</h2>'
            '<p class="sub">Every proposal ever created, newest first (index.json + proposal directories, '
            'including <code>_archive/</code>). &ldquo;not recorded&rdquo; = the decision predates actor logging; '
            'nothing is backfilled.</p>'
            f'<p>{stats}</p>'
            + _table(["proposal", "created", "pipeline", "check kind", "agent", "status",
                      "decided by", "decided at", "reason"], table_rows)
            + "</section>")


def render_governance(g: dict, now: dt.datetime | None = None, live: bool = False) -> str:
    now = now or dt.datetime.now()
    s = [f"<!doctype html><html lang=en><head><meta charset=utf-8>"
         f"<meta name=viewport content='width=device-width,initial-scale=1'>"
         f"<title>Pipeline Agent &mdash; Governance</title><style>{CSS}{EXTRA_CSS}</style></head><body><main>",
         nav("governance", live), "<h1>Governance</h1>",
         f"<p class=sub>Generated {e(now.isoformat(timespec='seconds'))} &middot; "
         "Claim: <b>nothing touches production without a logged human decision.</b> "
         "No auto-refresh on this page.</p>",
         '<section class=card><h2>Data contracts</h2>'
         '<p class="sub">Rendered from <code>manifests/*.yaml</code>; fields a manifest lacks are omitted.</p>',
         "".join(_contract_html(c) for c in g["contracts"]) or '<p class="mute">no manifests</p>',
         "</section>", _autonomy_html(g["autonomy"]), _audit_html(g["audit"], g["audit_counts"]),
         "</main></body></html>"]
    return "".join(s)
