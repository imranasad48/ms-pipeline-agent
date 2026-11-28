"""Shared layout + new pages: routes, nav, incident filters, per-pipeline pages, two-step approve,
static build with working relative links. Fixtures only; no agent, no LLM."""
from __future__ import annotations

import re
import urllib.parse
from pathlib import Path

import pytest
import yaml

from dashboard.__main__ import build
from dashboard.data import REPO, Paths
from dashboard.layout import NAV, fmt_ts
from dashboard.pages import route, static_routes
from dashboard.tests.test_data import world  # noqa: F401  (fixture re-use)
from dashboard.tests.test_server import _req, live  # noqa: F401  (fixture re-use)
from healing.proposals import list_proposals

PAGES = ["/", "/pipelines", "/pipeline/p_a", "/incidents", "/proposals", "/governance"]
FIVE = sorted(yaml.safe_load(p.read_text())["pipeline_id"] for p in (REPO / "manifests").glob("*.yaml"))


def _nav_ok(html: str) -> bool:
    return 'class="nav"' in html and all(f">{label}</a>" in html for _, label in NAV) and "Generated" in html


def _open_ids(html: str) -> set[str]:
    """Incident ids in the 'Open incidents' table of the incidents page."""
    sect = html.split("<h2>Open incidents", 1)[1].split("</section>", 1)[0]
    return set(re.findall(r"<code>(i\d+)</code>", sect))


# ------------------------------------------------------------------ routes
@pytest.mark.parametrize("path,needles", [
    ("/", ["Overview", "Pipelines", "Open incidents", "Healing proposals", "Latest evaluation"]),
    ("/pipelines", ["p_a", "p_b", "p_c", "checks", "contract"]),
    ("/pipeline/p_a", ["Contract", "Last load", "Open incidents", "Proposal history", "2026-11-02 01:00"]),
    ("/incidents", ["By severity", "By family", "Open incidents", "schema.renamed:a-&gt;b"]),
    ("/proposals", ["p-1", "p-2", "p-3", "READY_FOR_APPROVAL", "all (3)"]),
    ("/governance", ["Data contracts", "Autonomy policy", "Audit trail"]),
])
def test_each_route_renders_with_nav(world, path, needles):  # noqa: F811
    for live_mode in (True, False):
        code, html = route(world, path, {}, live=live_mode)
        assert code == 200, path
        assert _nav_ok(html), path
        for n in needles:
            assert n in html, (path, n)
        assert "http://" not in html and "https://" not in html      # no external assets
        assert "<script" not in html or (live_mode and path == "/")  # only the live Overview has JS


def test_nav_highlights_current_page(world):  # noqa: F811
    for path, label in [("/", "Overview"), ("/pipelines", "Pipelines"), ("/pipeline/p_a", "Pipelines"),
                        ("/incidents", "Incidents"), ("/proposals", "Proposals"), ("/governance", "Governance")]:
        html = route(world, path, {})[1]
        assert re.search(rf'aria-current=page>{label}</a>', html), path


def test_live_routes_over_http(live):  # noqa: F811
    toy, pid, port = live
    for path in ["/", "/pipelines", "/incidents", "/proposals", "/governance", "/incidents?severity=warn"]:
        status, _, html = _req(port, "GET", path)
        assert status == 200 and _nav_ok(html), path
    assert pid in _req(port, "GET", "/proposals")[2]


def test_overview_tiles_link_to_filtered_pages(world):  # noqa: F811
    html = route(world, "/", {}, live=True)[1]
    for href in ['href="/incidents"', 'href="/incidents?severity=critical"', 'href="/incidents?severity=warn"',
                 'href="/incidents?severity=info"', 'href="/pipelines"', 'href="/proposals?status=READY_FOR_APPROVAL"',
                 'href="/pipeline/p_a"']:
        assert href in html, href
    static = route(world, "/", {}, live=False)[1]
    assert 'href="incidents-critical.html"' in static and 'href="pipeline-p_a.html"' in static


# ------------------------------------------------------------------ incident filters
def test_each_incident_filter_narrows(world):  # noqa: F811
    allopen = _open_ids(route(world, "/incidents", {})[1])
    assert allopen == {"i1", "i2", "i3"}
    for q, expect in [({"severity": ["critical"]}, {"i2"}), ({"pipeline": ["p_b"]}, {"i3"}),
                      ({"family": ["schema"]}, {"i1", "i3"}), ({"date": ["2026-08-15"]}, {"i2", "i3"}),
                      ({"pipeline": ["p_a"], "family": ["schema"]}, {"i1"})]:
        html = route(world, "/incidents", q)[1]
        assert _open_ids(html) == expect, q
        assert "Active filters" in html and ">clear</a>" in html and 'href="/incidents"' in html


@pytest.mark.parametrize("q", [{"severity": ["bogus"]}, {"pipeline": ["nope"]}, {"family": ["x"]},
                               {"date": ["not-a-date"]}, {"date": ["2030-01-01"]},
                               {"severity": ["<script>alert(1)</script>"]}])
def test_unknown_filter_value_gives_empty_state(world, q):  # noqa: F811
    code, html = route(world, "/incidents", q)
    assert code == 200 and "No open incidents for this filter." in html and _open_ids(html) == set()
    assert "<script>alert(1)" not in html


def test_bad_filter_over_http_is_not_a_traceback(live):  # noqa: F811
    _, _, port = live
    status, _, html = _req(port, "GET", "/incidents?severity=%ff&date=&&pipeline=" + "x" * 300)
    assert status == 200 and "Traceback" not in html and "No open incidents for this filter." in html


def test_proposals_status_filter(world):  # noqa: F811
    html = route(world, "/proposals", {"status": ["APPLIED"]})[1]
    assert "p-1" in html and "p-2" not in html and "p-3" not in html and ">clear</a>" in html
    html = route(world, "/proposals", {"status": ["NOPE"]})[1]
    assert "No proposals with status" in html


# ------------------------------------------------------------------ per-pipeline
def _repo_paths(tmp_path) -> Paths:
    return Paths(manifests=REPO / "manifests", warehouse=tmp_path / "none.db", incidents=tmp_path / "none.db",
                 proposals=tmp_path / "index.json", eval_summary=tmp_path / "none.md", site=tmp_path / "site",
                 orch_status=tmp_path / "st.json")


def test_pipeline_page_for_each_of_the_five_manifests(tmp_path):
    assert len(FIVE) == 5
    p = _repo_paths(tmp_path)
    for pid in FIVE:
        m = yaml.safe_load((REPO / "manifests" / f"{pid}.yaml").read_text())
        code, html = route(p, f"/pipeline/{pid}", {})
        assert code == 200 and f"<h1>{pid}</h1>" in html
        assert f"<dd>{len(m.get('validation_checks') or [])}</dd>" in html        # number of checks
        assert m["destination"]["table"] in html
        for s in m.get("sources", []):
            assert s["id"] in html
        assert f'/governance?open={pid}#contract-{pid}' in html
        assert "No open incidents for this pipeline." in html and "No proposals for this pipeline yet." in html


def test_unknown_pipeline_is_404(tmp_path, live):  # noqa: F811
    code, html = route(_repo_paths(tmp_path), "/pipeline/nope", {})
    assert code == 404 and "No pipeline" in html and _nav_ok(html)
    _, _, port = live
    status, _, html = _req(port, "GET", "/pipeline/%3Cnope%3E")
    assert status == 404 and "<nope>" not in html and "Traceback" not in html


def test_governance_open_param_expands_that_contract(tmp_path):
    html = route(_repo_paths(tmp_path), "/governance", {"open": [FIVE[0]]})[1]
    assert f'<div id="contract-{FIVE[0]}"><details class="card" open>' in html
    assert html.count('<details class="card" open>') == 1


# ------------------------------------------------------------------ approve / reject
def test_approve_without_confirm_step_is_refused(live):  # noqa: F811
    toy, pid, port = live
    for body in ({"proposal_id": pid}, {"proposal_id": pid, "confirm": "yes"}):
        status, loc, _ = _req(port, "POST", "/approve", body)
        assert status == 303 and "err=1" in loc and "confirm" in urllib.parse.unquote_plus(loc)
    assert {p["proposal_id"]: p["status"] for p in list_proposals(toy.settings)}[pid] == "READY_FOR_APPROVAL"


def test_approve_is_a_two_step_inline_control(live):  # noqa: F811
    _, pid, port = live
    html = _req(port, "GET", "/")[2]
    card = html.split("Awaiting your approval", 1)[1].split("</section>", 1)[0]
    step = card.split('<details class="confirm">', 1)[1].split("</details>", 1)[0]
    assert "Approve &amp; apply" in step.split("</summary>")[0]                 # first click opens it
    assert 'action="/approve"' in step and 'name="confirm" value="apply"' in step
    assert "Confirm apply to production" in step and "dags/toy/pipeline.py" in step  # what will change
    assert card.count('action="/approve"') == 1                                  # no one-click path
    assert not re.search(r"\b(alert|confirm|prompt)\(", html)                    # no browser dialogs


def test_reject_without_reason_still_refused_and_textarea_required(live):  # noqa: F811
    toy, pid, port = live
    _, loc, _ = _req(port, "POST", "/reject", {"proposal_id": pid})
    assert "err=1" in loc and "reason is required" in urllib.parse.unquote_plus(loc)
    assert {p["proposal_id"]: p["status"] for p in list_proposals(toy.settings)}[pid] == "READY_FOR_APPROVAL"
    assert re.search(r'<textarea name="reason"[^>]*required', _req(port, "GET", "/")[2])


def test_refresh_script_pauses_for_input_and_keeps_scroll(live):  # noqa: F811
    _, _, port = live
    html = _req(port, "GET", "/")[2]
    js = html.split("<script>", 1)[1].split("</script>", 1)[0]
    assert "activeElement" in js and ".value" in js and "details[open]" in js    # pause conditions
    assert "scrollTo" in js and "location.reload" not in js and "http-equiv" not in html
    for path in ("/pipelines", "/incidents", "/proposals", "/governance"):
        assert "<script" not in _req(port, "GET", path)[2]


# ------------------------------------------------------------------ formatting
def test_timestamp_format():
    assert fmt_ts("2026-11-28T02:30:41Z") == "2026-11-28 02:30"
    assert fmt_ts("2026-11-28 02:30:41.123") == "2026-11-28 02:30"
    assert fmt_ts("2026-08-14") == "2026-08-14" and fmt_ts("not recorded") == "not recorded"
    assert fmt_ts(None) == ""


def test_no_iso_timestamps_leak_into_pages(world):  # noqa: F811
    for path in PAGES:
        html = route(world, path, {})[1]
        text = re.sub(r"<[^>]+>", " ", html)
        assert not re.search(r"\d{4}-\d\d-\d\dT\d\d:\d\d", text), path


# ------------------------------------------------------------------ static build
def test_static_build_writes_every_page_with_working_links(world):  # noqa: F811
    build(world)
    names = set(static_routes(world))
    written = {p.name for p in world.site.glob("*.html")}
    assert names <= written
    for need in ["index.html", "pipelines.html", "incidents.html", "proposals.html", "governance.html",
                 "pipeline-p_a.html", "pipeline-p_b.html", "pipeline-p_c.html", "incidents-critical.html",
                 "proposals-READY_FOR_APPROVAL.html"]:
        assert need in written, need
    for f in world.site.glob("*.html"):
        html = f.read_text()
        assert "<script" not in html and "<form" not in html and _nav_ok(html), f.name
        for href in re.findall(r'href="([^"]+)"', html):
            target = href.split("#")[0]
            assert not target.startswith("/"), (f.name, href)                 # relative only
            assert (world.site / target).exists(), (f.name, href)


def test_dump_unchanged_shape(world, capsys):  # noqa: F811
    from dashboard.__main__ import dump
    out = dump(world)
    for head in ("== PIPELINES ==", "== OPEN INCIDENTS: 3 of 4 ==", "== HEALING PROPOSALS ==",
                 "== LATEST EVALUATION =="):
        assert head in out


def test_dashboard_never_reads_error_manifest():
    """Only catalog.py (to skip it by name) and jobs.py (to withhold log lines / tell the agent not to
    open it) may mention it, and no line that mentions it opens or reads a file."""
    for p in Path(REPO / "dashboard").glob("*.py"):
        lines = [ln for ln in p.read_text().splitlines() if "error_manifest" in ln]
        if p.name not in ("catalog.py", "jobs.py"):
            assert not lines, p.name
        for ln in lines:
            assert not re.search(r"open\(|read_text|read_bytes|json\.load|glob\(", ln), (p.name, ln)
