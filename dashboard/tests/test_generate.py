"""Generate a pipeline from the browser: catalog, validation, jobs, load, history, rollback.

Everything uses dashboard/tests/fake_generator.py in temp dirs. The real generation agent is
never run (no LLM, no network, no subscription usage)."""
from __future__ import annotations

import csv
import http.client
import json
import os
import re
import sqlite3
import subprocess
import sys
import textwrap
import threading
import time
import urllib.parse
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from dashboard.catalog import build_catalog
from dashboard.data import Paths
from dashboard.jobs import (GenConfig, JobManager, Refused, _walk, compose_brief, now_iso, snapshot,
                            validate_request)
from dashboard.pages import route, static_routes
from dashboard.server import make_server
from healing.settings import Settings

FAKE = Path(__file__).with_name("fake_generator.py")
PROMPT = "Load the daily customer snapshots into one table, one row per customer."


def gen(mode: str) -> list:
    return [sys.executable, str(FAKE), "--mode", mode]


def _csv(path: Path, header: list, rows: list) -> None:
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


@pytest.fixture
def gw(tmp_path):
    repo = tmp_path / "repo"
    (repo / "manifests").mkdir(parents=True)
    (repo / "dags" / "orders").mkdir(parents=True)
    (repo / "dags" / "orders" / "pipeline.py").write_text("X = 1\n")
    (repo / "dags" / "orders_dag.py").write_text("# orders dag\n")
    (repo / "manifests" / "orders.yaml").write_text(yaml.safe_dump({
        "pipeline_id": "orders", "version": 1, "destination": {"table": "orders"},
        "sources": [{"id": "so", "path_pattern": "${DATA_ROOT}/crm/sales_orders_*.csv"}],
        "validation_checks": [{"id": "c1"}]}))
    data = tmp_path / "data"
    (data / "crm").mkdir(parents=True)
    for i, d in enumerate(("2026-08-14", "2026-08-15", "2026-08-16")):
        _csv(data / "crm" / f"customers_{d}.csv", ["id", "name"], [[f"c{i}{j}", f"n{j}"] for j in range(4)])
        _csv(data / "crm" / f"sales_orders_{d}.csv", ["order_id"], [[i]])
    (data / "erp").mkdir()
    con = sqlite3.connect(data / "erp" / "erp.db")
    con.execute("CREATE TABLE products (a, b, c, d, e)")
    con.execute("CREATE TABLE suppliers (a, b, c)")
    con.commit()
    con.close()
    (data / "3pl").mkdir()
    (data / "3pl" / "fleet.json").write_text("[]")
    wh = tmp_path / "wh.db"
    con = sqlite3.connect(wh)
    con.execute("CREATE TABLE orders (x)")
    con.execute("CREATE TABLE customers (keep)")
    con.execute("INSERT INTO customers VALUES ('original')")
    con.commit()
    con.close()
    cfg = GenConfig(repo_root=repo, data_root=data, warehouse=wh, jobs_dir=tmp_path / "jobs",
                    history=tmp_path / "history.jsonl", generator=gen("ok"), time_limit_s=60, kill_grace_s=1)
    paths = Paths(manifests=repo / "manifests", warehouse=wh, incidents=tmp_path / "none.db",
                  proposals=tmp_path / "index.json", eval_summary=tmp_path / "none.md", site=tmp_path / "site",
                  orch_status=tmp_path / "st.json", history=cfg.history)
    return SimpleNamespace(repo=repo, data=data, wh=wh, cfg=cfg, paths=paths, tmp=tmp_path)


def form(pid="customers_daily", sources=("crm/customers_*.csv",), prompt=PROMPT) -> dict:
    return {"source": list(sources), "pipeline_id": [pid], "prompt": [prompt]}


def tree(root: Path) -> dict:
    """dags/ + manifests/ content, ignoring bytecode caches."""
    files, dirs = _walk(root)
    return {"files": {k: v for k, v in files.items() if v != "pyc"},
            "dirs": sorted(d for d in dirs if "__pycache__" not in d)}


def until(cond, timeout=20.0):
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return True
        time.sleep(0.05)
    raise AssertionError("condition not reached")


def tables(db: Path) -> set:
    con = sqlite3.connect(db)
    try:
        return {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    finally:
        con.close()


# ------------------------------------------------------------------ catalog
def test_catalog_groups_files_tables_and_coverage(gw):
    cat = build_catalog(gw.data, gw.repo / "manifests")
    by = {s["key"]: s for s in cat}
    assert set(by) == {"crm/customers_*.csv", "crm/sales_orders_*.csv", "erp/erp.db#products",
                       "erp/erp.db#suppliers", "3pl/fleet.json"}
    c = by["crm/customers_*.csv"]
    assert c["count"] == 3 and c["date_range"] == ("2026-08-14", "2026-08-16") and c["covered_by"] == []
    assert by["crm/sales_orders_*.csv"]["covered_by"] == ["orders"]
    assert by["erp/erp.db#products"]["columns"] == 5 and by["erp/erp.db#suppliers"]["columns"] == 3
    assert [bool(s["covered_by"]) for s in cat] == sorted(bool(s["covered_by"]) for s in cat)  # uncovered first
    assert c["suggest"] == "customers" and by["erp/erp.db#products"]["suggest"] == "erp_products"
    assert "error_manifest" not in json.dumps(cat)


def test_catalog_week_files_and_never_error_manifest(tmp_path):
    (tmp_path / "manual").mkdir()
    for w in ("2026-W33", "2026-W35"):
        (tmp_path / "manual" / f"stock_count_{w}.xlsx").write_text("x")
    (tmp_path / "error_manifest.json").write_text("{}")
    (tmp_path / "manual" / "error_manifest.csv").write_text("x")
    cat = build_catalog(tmp_path, tmp_path / "nomanifests")
    assert [(s["key"], s["count"], s["date_range"]) for s in cat] == \
        [("manual/stock_count_*.xlsx", 2, ("2026-W33", "2026-W35"))]


# ------------------------------------------------------------------ validation
@pytest.mark.parametrize("f,needle", [
    (form(sources=["crm/nope_*.csv"]), "Unknown source"),
    (form(sources=["../../etc/passwd"]), "Unknown source"),
    (form(sources=["crm/../../../etc/passwd"]), "Unknown source"),
    (form(sources=["/etc/passwd"]), "Unknown source"),
    (form(sources=[]), "at least one source"),
    (form(pid="Bad-Id"), "lowercase"),
    (form(pid="1abc"), "lowercase"),
    (form(pid="../x"), "lowercase"),
    (form(pid="orders"), "already exists"),
    (form(pid="json"), "shadow"),
    (form(prompt="   "), "Describe"),
    (form(prompt="x" * 4001), "limit"),
])
def test_validation_refuses(gw, f, needle):
    _, errors = validate_request(gw.cfg, f)
    assert any(needle in e for e in errors), errors
    m = JobManager(gw.cfg)
    before = tree(gw.repo)
    with pytest.raises(Refused):
        m.submit(f)
    assert tree(gw.repo) == before and m.list() == []


def test_existing_dag_without_manifest_also_blocks_id(gw):
    (gw.repo / "dags" / "legacy_dag.py").write_text("#\n")
    _, errors = validate_request(gw.cfg, form(pid="legacy"))
    assert any("already exists" in e for e in errors)


def test_brief_keeps_prompt_and_names_id_and_sources(gw):
    req, errors = validate_request(gw.cfg, form(sources=["crm/customers_*.csv", "erp/erp.db#products"]))
    assert not errors
    b = compose_brief(req)
    assert b.startswith(PROMPT) and "Pipeline id: customers_daily" in b
    assert "crm/customers_*.csv  (3 files, 2026-08-14 .. 2026-08-16)" in b and "SQLite table `products`" in b


# ------------------------------------------------------------------ happy path
def test_happy_path_generates_loads_records_and_shows(gw, monkeypatch):
    monkeypatch.setenv("PIPELINE_AGENT_USER", "alice")
    m = JobManager(gw.cfg)
    jid = m.submit(form())
    job = m.wait(jid, 60)
    assert job["status"] == "succeeded", job["error"]
    assert job["rows_loaded"] == 12 and job["destination_table"] == "customers_daily_t" and job["exit_code"] == 0
    assert "customers_daily_t" in tables(gw.wh)
    for f in ("dags/customers_daily/pipeline.py", "dags/customers_daily_dag.py", "manifests/customers_daily.yaml"):
        assert (gw.repo / f).exists()
    log = m.read_log(jid, 0, whole_lines=False)[0]
    assert "FINAL STATUS: SUCCESS" in log and "loaded 12 rows" in log
    assert "error_manifest" not in log and "line withheld" in log
    hist = [json.loads(x) for x in gw.cfg.history.read_text().splitlines()]
    assert len(hist) == 1
    h = hist[0]
    assert (h["pipeline_id"], h["actor"], h["channel"], h["outcome"], h["rows_loaded"]) == \
        ("customers_daily", "alice", "dashboard", "succeeded", 12)
    assert h["sources"] == ["crm/customers_*.csv"] and h["prompt"] == PROMPT and h["started_at"] and h["finished_at"]
    code, html = route(gw.paths, "/pipeline/customers_daily", {}, live=True)
    assert code == 200 and "Created by <b>alice</b> via dashboard" in html and PROMPT in html
    assert "dags/customers_daily_dag.py" in html and f"/jobs/{jid}" in html
    assert "12" in html                                                    # row count on the page
    code, html = route(gw.paths, "/pipeline/orders", {}, live=True)
    assert "Provenance: not recorded" in html
    gov = route(gw.paths, "/governance", {}, live=True)[1]
    assert "Pipeline provenance" in gov and "alice via dashboard" in gov and "not recorded" in gov


# ------------------------------------------------------------------ failures roll back
@pytest.mark.parametrize("mode,needle", [
    ("report_fail", "FINAL STATUS: FAILED could not infer a key"),
    ("crash", "exited with code 3"),
    ("no_status", "without reporting"),
    ("bad_load", "Loading failed at step 'load'"),
    ("touch", "changed existing files"),
    ("table_clash", "already exists in the warehouse"),
])
def test_failure_restores_dags_and_manifests(gw, mode, needle):
    gw.cfg.generator = gen(mode)
    before = tree(gw.repo)
    tables_before = tables(gw.wh)
    m = JobManager(gw.cfg)
    jid = m.submit(form())
    job = m.wait(jid, 60)
    assert job["status"] == "failed" and needle in job["error"], job["error"]
    assert tree(gw.repo) == before                                    # exactly as before
    q = gw.cfg.jobs_dir / jid / "quarantine"
    assert (q / "dags" / "customers_daily" / "pipeline.py").exists()  # moved, not deleted
    assert tables(gw.wh) == tables_before
    con = sqlite3.connect(gw.wh)
    assert con.execute("SELECT keep FROM customers").fetchall() == [("original",)]
    con.close()
    assert json.loads(gw.cfg.history.read_text().splitlines()[-1])["outcome"] == "failed"
    assert m.active() is None


def test_cancel_stops_process_group_and_restores(gw):
    gw.cfg.generator = gen("sleep")
    before = tree(gw.repo)
    m = JobManager(gw.cfg)
    jid = m.submit(form())
    until(lambda: "working 3" in m.read_log(jid)[0])
    pid = m.get(jid)["pid"]
    assert m.get(jid)["status"] == "generating"
    m.cancel(jid)
    job = m.wait(jid, 30)
    assert job["status"] == "cancelled" and "Cancelled" in job["error"]
    assert tree(gw.repo) == before
    assert (gw.cfg.jobs_dir / jid / "quarantine" / "manifests" / "customers_daily.yaml").exists()
    until(lambda: subprocess.run(["ps", "-p", str(pid)], capture_output=True).returncode != 0, 10)


def test_time_limit_fails_and_restores(gw):
    gw.cfg.generator = gen("sleep")
    gw.cfg.time_limit_s = 1.0
    before = tree(gw.repo)
    m = JobManager(gw.cfg)
    job = m.wait(m.submit(form()), 30)
    assert job["status"] == "failed" and "Time limit" in job["error"]
    assert tree(gw.repo) == before


def test_second_job_refused_while_one_runs(gw):
    gw.cfg.generator = gen("sleep")
    m = JobManager(gw.cfg)
    jid = m.submit(form())
    with pytest.raises(Refused) as ex:
        m.submit(form(pid="other_one"))
    assert "already running" in ex.value.args[0][0]
    m.cancel(jid)
    assert m.wait(jid, 30)["status"] == "cancelled"
    gw.cfg.generator = gen("ok")
    assert m.wait(m.submit(form(pid="other_one")), 60)["status"] == "succeeded"   # free again


def test_restart_mid_job_marks_interrupted_and_restores(gw):
    gw.cfg.generator = gen("sleep")
    before = tree(gw.repo)
    procs = []

    class Crashes(JobManager):        # starts the generator, then "the server dies"
        def _run(self, job_id):
            job = self.get(job_id)
            snapshot(Path(self.cfg.repo_root), self.job_dir(job_id))
            self._update(job_id, status="generating", started_at=now_iso())
            p = subprocess.Popen(self.cfg.job_argv(job["brief"]), cwd=self.cfg.repo_root,
                                 stdout=subprocess.DEVNULL, start_new_session=True)
            procs.append(p)
            self._update(job_id, pid=p.pid)

    jid = Crashes(gw.cfg).submit(form())
    until(lambda: (gw.repo / "manifests" / "customers_daily.yaml").exists())
    assert json.loads((gw.cfg.jobs_dir / jid / "job.json").read_text())["status"] == "generating"
    m2 = JobManager(gw.cfg)                                           # server restart
    job = m2.get(jid)
    assert job["status"] == "interrupted" and "server stopped" in job["error"]
    procs[0].wait(10)                                                 # orphan was stopped
    assert tree(gw.repo) == before
    assert (gw.cfg.jobs_dir / jid / "quarantine" / "dags" / "customers_daily" / "pipeline.py").exists()
    assert json.loads(gw.cfg.history.read_text().splitlines()[-1])["outcome"] == "interrupted"
    assert m2.active() is None


def test_prompt_with_shell_metacharacters_is_data(gw):
    nasty = ('Load customers"; touch PWNED; $(touch PWNED2) `touch PWNED3` && echo $HOME | tee x \' ; '
             "rm -rf ./dags & > /dev/null\nsecond line \\n {braces} %s")
    m = JobManager(gw.cfg)
    jid = m.submit(form(prompt=nasty))
    job = m.wait(jid, 60)
    assert job["status"] == "succeeded", job["error"]
    log = m.read_log(jid, 0, whole_lines=False)[0]
    assert nasty in log.split("BRIEF-BEGIN\n", 1)[1]                  # arrived verbatim as one argv item
    for p in (gw.repo, Path.cwd(), gw.tmp):
        assert not any((p / n).exists() for n in ("PWNED", "PWNED2", "PWNED3", "x"))
    assert (gw.repo / "dags" / "orders" / "pipeline.py").exists()


# ------------------------------------------------------------------ HTTP
@pytest.fixture
def server(gw):
    m = JobManager(gw.cfg)
    srv = make_server(gw.paths, Settings(), 0, jobs=m)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield gw, m, srv.server_address[1]
    srv.shutdown()
    srv.server_close()


def _req(port, method, path, body=None, headers=None):
    c = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    c.request(method, path, body=urllib.parse.urlencode(body, doseq=True) if body is not None else None,
              headers={"Content-Type": "application/x-www-form-urlencoded", **(headers or {})})
    r = c.getresponse()
    data = r.read().decode()
    c.close()
    return r.status, r.getheader("Location"), data


def test_new_page_lists_catalog_and_nav_cta(server):
    gw, _, port = server
    status, _, html = _req(port, "GET", "/new")
    assert status == 200 and "crm/customers_*.csv" in html and "erp/erp.db#products" in html
    assert "no pipeline yet" in html and 'href="/pipeline/orders"' in html
    assert html.index("crm/customers_*.csv") < html.index("crm/sales_orders_*.csv")   # uncovered first
    assert "error_manifest" not in html.replace("error_manifest.*", "")              # only the rule text
    assert 'class="btn primary cta" href="/new"' in html
    assert 'class="btn primary cta"' in _req(port, "GET", "/")[2]
    assert not re.search(r"\b(alert|confirm|prompt)\(", html)


def test_generate_over_http_end_to_end(server):
    gw, m, port = server
    status, loc, _ = _req(port, "POST", "/generate", form())
    assert status == 303 and loc.startswith("/jobs/g-")
    jid = loc.rsplit("/", 1)[1]
    m.wait(jid, 60)
    status, _, html = _req(port, "GET", loc)
    assert status == 200 and "succeeded" in html and 'href="/pipeline/customers_daily"' in html
    assert "was not triggered" in html
    poll = json.loads(_req(port, "GET", f"{loc}/poll?offset=0")[2])
    assert poll["status"] == "succeeded" and poll["terminal"] and "FINAL STATUS: SUCCESS" in poll["log"]
    assert json.loads(_req(port, "GET", f"{loc}/poll?offset={poll['offset']}")[2])["log"] == ""
    status, _, html = _req(port, "GET", "/pipeline/customers_daily")
    assert status == 200 and "Created by" in html
    assert jid in _req(port, "GET", "/jobs")[2]


def test_http_refusals(server):
    gw, m, port = server
    s, _, _ = _req(port, "POST", "/generate", form(), {"Origin": "http://evil.example"})
    assert s == 403 and m.list() == []
    s, _, html = _req(port, "POST", "/generate", form(sources=["../../etc/passwd"]))
    assert s == 400 and "Unknown source" in html and PROMPT in html          # form kept
    s, _, html = _req(port, "POST", "/generate", form(pid="orders"))
    assert s == 400 and "already exists" in html
    assert _req(port, "GET", "/jobs/g-nope")[0] == 404
    assert _req(port, "GET", "/jobs/../../etc/passwd")[0] == 404
    assert _req(port, "POST", "/jobs/g-20260101-000000-abcd/cancel", {})[0] == 404
    assert m.list() == []


def test_server_responsive_and_cancel_over_http_while_job_runs(server):
    gw, m, port = server
    gw.cfg.generator = gen("sleep")
    _, loc, _ = _req(port, "POST", "/generate", form())
    jid = loc.rsplit("/", 1)[1]
    until(lambda: "working 2" in m.read_log(jid)[0])
    t = time.time()
    assert _req(port, "GET", "/")[0] == 200 and time.time() - t < 5
    s, _, html = _req(port, "POST", "/generate", form(pid="another"))
    assert s == 409 and "already running" in html
    html = _req(port, "GET", loc)[2]
    assert "Cancel job" in html and 'id="log"' in html and "fetch(" in html
    poll = json.loads(_req(port, "GET", f"{loc}/poll?offset=0")[2])
    assert poll["status"] == "generating" and not poll["terminal"] and "working" in poll["log"]
    s, loc2, _ = _req(port, "POST", f"{loc}/cancel", {})
    assert s == 303 and loc2 == loc
    assert m.wait(jid, 30)["status"] == "cancelled"
    html = _req(port, "GET", loc)[2]
    assert "Cancelled" in html and "Cancel job" not in html


def test_static_build_has_no_generation_pages_or_links(gw):
    names = set(static_routes(gw.paths))
    assert not any(n.startswith(("new", "jobs")) for n in names)
    for name, (path, q) in static_routes(gw.paths).items():
        html = route(gw.paths, path, q, live=False)[1]
        assert 'href="/new"' not in html and 'href="/jobs' not in html and "New pipeline" not in html


def test_dates_never_wrap_in_tables(gw):
    from dashboard.layout import table
    html = table(["date", "ts", "x"], [["2026-11-09", "2026-11-09 02:30", "abc"]])
    assert html.count('class="nowrap"') == 2



# ------------------------------------------------------------------ inspection root + guards
def _agent_module():
    import importlib.util
    spec = importlib.util.spec_from_file_location("generate_pipeline", REPO_ROOT / "agent" / "generate_pipeline.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


REPO_ROOT = Path(__file__).resolve().parents[2]


def test_job_passes_its_own_data_root_to_the_agent(gw):
    m = JobManager(gw.cfg)
    jid = m.submit(form())
    job = m.wait(jid, 60)
    assert job["status"] == "succeeded", job["error"]
    log = m.read_log(jid, 0, whole_lines=False)[0]
    assert f"DATA-ROOT: {gw.data.resolve()}" in log
    i = job["command"].index("--data-root")
    assert job["command"][i + 1] == str(gw.data.resolve()) and job["command"][-2] == "--brief"
    assert gw.cfg.job_argv("b")[-4:] == ["--data-root", str(gw.data.resolve()), "--brief", "b"]
    default = GenConfig(data_root=gw.data).job_argv("b")
    assert default[1].endswith("agent/generate_pipeline.py") and "--data-root" in default


def test_agent_data_root_flag_default_unchanged_and_custom_root_everywhere():
    g = _agent_module()
    assert g.DEFAULT_DATA_ROOT == "../data_generation/data_clean"
    assert g.system_prompt() == g.SYSTEM                        # default: byte-identical instructions
    custom = g.system_prompt("/abs/demo_root")
    assert "data_generation" not in custom
    assert custom.count("/abs/demo_root") == 4
    assert "DATA_ROOT=/abs/demo_root WAREHOUSE_DB=./warehouse/test_build.db" in custom
    assert "/opt/testbed/data_clean" in custom                  # container paths untouched


def test_agent_cli_reads_flag_then_env(monkeypatch):
    import argparse
    g = _agent_module()
    seen = {}
    monkeypatch.setattr(g.anyio, "run", lambda fn, brief, root: seen.update(brief=brief, root=root))
    monkeypatch.delenv("PIPELINE_GEN_DATA_ROOT", raising=False)
    src = textwrap.dedent((REPO_ROOT / "agent" / "generate_pipeline.py").read_text()
                          .split('if __name__ == "__main__":')[1])
    run = lambda argv: exec(src.replace("ap.parse_args()", "ap.parse_args(argv)"),  # noqa: E731
                            {**vars(g), "argparse": argparse, "argv": argv})
    run(["--brief", "x"])
    assert seen == {"brief": "x", "root": "../data_generation/data_clean"}
    monkeypatch.setenv("PIPELINE_GEN_DATA_ROOT", "/env/root")
    run(["--brief", "x"])
    assert seen["root"] == "/env/root"
    run(["--brief", "x", "--data-root", "/flag/root"])
    assert seen["root"] == "/flag/root"


def test_answer_key_in_data_root_refuses_generation(gw):
    (gw.data / "erp" / "deep").mkdir()
    (gw.data / "erp" / "deep" / "error_manifest.json").write_text('{"secret": true}')
    _, errors = validate_request(gw.cfg, form())
    assert any("answer key" in e for e in errors)
    m = JobManager(gw.cfg)
    before = tree(gw.repo)
    with pytest.raises(Refused) as ex:
        m.submit(form())
    assert "answer key" in ex.value.args[0][0] and "deep" not in ex.value.args[0][0]
    assert m.list() == [] and tree(gw.repo) == before
    cat = build_catalog(gw.data, gw.repo / "manifests")
    assert "error_manifest" not in json.dumps(cat)                # still never listed


def test_answer_key_appearing_after_submit_stops_before_agent_starts(gw):
    import dashboard.jobs as J
    m = JobManager(gw.cfg)
    orig = J.snapshot

    def snap(root, jd):                      # the file shows up between validation and launch
        (gw.data / "error_manifest.csv").write_text("x")
        return orig(root, jd)

    J.snapshot = snap
    try:
        job = m.wait(m.submit(form()), 30)
    finally:
        J.snapshot = orig
    assert job["status"] == "failed" and "answer key" in job["error"] and job["exit_code"] is None
    assert "fake generator starting" not in m.read_log(job["job_id"], 0, whole_lines=False)[0]


def test_default_time_limit_is_40_minutes(monkeypatch):
    monkeypatch.delenv("PIPELINE_GEN_TIME_LIMIT_MIN", raising=False)
    assert GenConfig().time_limit_s == 40 * 60
    monkeypatch.setenv("PIPELINE_GEN_TIME_LIMIT_MIN", "5")
    assert GenConfig().time_limit_s == 5 * 60


def test_cli_apply_refused_while_generating(tmp_path):
    from dashboard.jobs import active_job_on_disk, refuse_if_generating
    d = tmp_path / "jobs" / "g-20261128-010101-abcd"
    d.mkdir(parents=True)
    job = {"job_id": d.name, "pipeline_id": "p", "status": "generating", "pid": os.getpid()}
    (d / "job.json").write_text(json.dumps(job))
    with pytest.raises(SystemExit) as ex:
        refuse_if_generating(tmp_path / "jobs")
    assert "approve after it finishes" in str(ex.value)
    job.update(pid=2 ** 22 + 12345)                                 # recorded process is gone
    (d / "job.json").write_text(json.dumps(job))
    assert active_job_on_disk(tmp_path / "jobs") is None
    job.update(status="succeeded", pid=os.getpid())
    (d / "job.json").write_text(json.dumps(job))
    refuse_if_generating(tmp_path / "jobs")                          # no exception
