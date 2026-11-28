"""New pipelines run through Airflow: preflight, the orchestrating step, failures, the
"Enable schedule and run now" route, and pages that never wait for Airflow.

Fake Airflow (fake_airflow.py) + fake generator + temp dirs only: no docker, no LLM."""
from __future__ import annotations

import json
import shutil
import sqlite3
import threading
import time

import pytest

from dashboard.airflow import NOT_RUNNING, StatusCache, orchestrate
from dashboard.jobs import JobManager, Refused, compose_brief, validate_request
from dashboard.layout import fmt_utc
from dashboard.pages import route
from dashboard.server import make_server
from dashboard.tests.fake_airflow import FakeAirflow
from dashboard.tests.test_generate import _req, form, gen, gw, tables, tree  # noqa: F401
from healing.settings import Settings


def _actions(af):
    return [c[0] for c in af.calls if c[0] in ("unpause", "trigger", "pause")]


# ------------------------------------------------------------------ preflight
def test_preflight_refuses_when_airflow_is_down_before_any_agent_usage(gw):
    gw.af.available = False
    m = JobManager(gw.cfg, gw.af)
    before = tree(gw.repo)
    with pytest.raises(Refused) as ex:
        m.submit(form())
    assert ex.value.args[0] == [NOT_RUNNING] and "docker compose up -d" in NOT_RUNNING
    assert m.list() == [] and tree(gw.repo) == before                # no job, the agent never started
    assert not gw.cfg.history.exists()


def test_preflight_refuses_when_sources_differ_from_what_airflow_reads(gw, tmp_path):
    other = tmp_path / "airflow_data"
    shutil.copytree(gw.data, other)
    gw.cfg.airflow_data = other
    assert validate_request(gw.cfg, form())[1] == []                  # identical copies are fine
    (other / "crm" / "customers_2026-08-15.csv").write_text("id,name\nzz,changed\n")
    _, errors = validate_request(gw.cfg, form())
    assert any("differ between the data root" in e and "crm/customers_2026-08-15.csv" in e for e in errors), errors
    _, errors = validate_request(gw.cfg, form(sources=["erp/erp.db#products"]))
    assert errors == []                                              # only the selected sources count
    (other / "erp" / "erp.db").unlink()
    assert any("erp/erp.db" in e for e in validate_request(gw.cfg, form(sources=["erp/erp.db#products"]))[1])


def test_preflight_refuses_a_warehouse_airflow_does_not_write(gw, tmp_path):
    gw.cfg.airflow_warehouse = tmp_path / "elsewhere.db"
    assert any("not the file Airflow writes" in e for e in validate_request(gw.cfg, form())[1])


def test_brief_states_daily_schedule_catchup_off_and_utc_loaded_at(gw):
    req, _ = validate_request(gw.cfg, form())
    b = compose_brief(req)
    assert 'schedule="@daily"' in b and "catchup=False" in b and "loaded_at" in b and "UTC" in b


# ------------------------------------------------------------------ happy path
def test_happy_path_goes_through_orchestrating(gw):
    seen = []
    m = JobManager(gw.cfg, gw.af)
    orig = gw.af.unpause

    def unpause(dag_id):                                             # observe the job's status mid-run
        seen.append(m.get(jid)["status"])
        orig(dag_id)
    gw.af.unpause = unpause
    jid = m.submit(form())
    job = m.wait(jid, 60)
    assert job["status"] == "succeeded", job["error"]
    assert seen == ["orchestrating"]
    assert _actions(gw.af) == ["unpause", "trigger"]
    assert job["rows_loaded"] == 12 and "customers_daily_t" in tables(gw.wh)
    assert job["airflow_schedule"] == "0 0 * * *" and job["airflow_catchup"] is False and job["dag_enabled"]
    assert job["airflow_data"].startswith("/opt/testbed/data_clean")
    assert job["warnings"] == []                                     # the table has loaded_at
    names = [c[0] for c in gw.af.calls]
    assert names.index("parse") < names.index("unpause") < names.index("trigger") < names.index("run")


def test_dag_not_registered_yet_is_registered_then_run(gw):
    gw.af.register_after = 1
    m = JobManager(gw.cfg, gw.af)
    job = m.wait(m.submit(form()), 60)
    assert job["status"] == "succeeded", job["error"]
    assert ("parse", "customers_daily", True) in gw.af.calls


# ------------------------------------------------------------------ failures
@pytest.mark.parametrize("knob,needle,unpaused", [
    ({"import_error": "ModuleNotFoundError: No module named 'openpyxl2'"}, "cannot import the DAG", False),
    ({"catchup": True}, "catchup=True", False),
    ({"outcome": "failed"}, "failed at task 'transform'", True),
    ({"outcome": "hang"}, "Time limit reached", True),
])
def test_airflow_failures_fail_pause_and_roll_back(gw, knob, needle, unpaused):
    for k, v in knob.items():
        setattr(gw.af, k, v)
    gw.cfg.time_limit_s = 3
    before, tables_before = tree(gw.repo), tables(gw.wh)
    m = JobManager(gw.cfg, gw.af)
    job = m.wait(m.submit(form()), 60)
    assert job["status"] == "failed" and needle in job["error"], job["error"]
    assert gw.af.paused.get("customers_daily", True) is True         # never left enabled
    assert _actions(gw.af) == (["unpause", "trigger", "pause"] if unpaused else [])
    assert tree(gw.repo) == before and tables(gw.wh) == tables_before
    assert (gw.cfg.jobs_dir / job["job_id"] / "quarantine" / "dags" / "customers_daily" / "pipeline.py").exists()
    assert job["dag_enabled"] is False
    h = json.loads(gw.cfg.history.read_text().splitlines()[-1])
    assert h["outcome"] == "failed" and h["airflow_run_id"] == f"dashboard__{job['job_id']}"
    if knob.get("outcome") == "failed":
        assert job["airflow_failed_task"] == "transform" and "KeyError: 'customer_id'" in job["airflow_log_tail"]
        html = route(gw.paths, "/", {})[1]                           # (overview still renders)
        assert html
    if knob.get("import_error"):
        assert "openpyxl2" in job["error"] and "openpyxl2" in job["airflow_log_tail"]


def test_failed_run_shows_task_and_log_tail_on_job_page(gw):
    from dashboard.genpages import render_job
    gw.af.outcome = "failed"
    m = JobManager(gw.cfg, gw.af)
    jid = m.submit(form())
    m.wait(jid, 60)
    html = render_job(m, jid, set())
    assert "Airflow task <code>transform</code> log (tail)" in html and "KeyError: &#x27;customer_id&#x27;" in html
    assert "the Airflow DAG <code>customers_daily</code> is paused" in html and "restored" in html


def test_cancel_while_orchestrating_pauses_and_rolls_back(gw):
    gw.af.outcome = "hang"
    m = JobManager(gw.cfg, gw.af)
    jid = m.submit(form())
    end = time.time() + 30
    while time.time() < end and "trigger" not in [c[0] for c in gw.af.calls]:
        time.sleep(0.02)
    m.cancel(jid)
    job = m.wait(jid, 30)
    assert job["status"] == "cancelled" and gw.af.paused["customers_daily"] is True
    assert not (gw.repo / "manifests" / "customers_daily.yaml").exists()


def test_orchestrate_never_touches_protected_dags():
    af = FakeAirflow()
    res = orchestrate(af, "stock_counts", "dashboard__x", time.monotonic() + 5, log=lambda t: None)
    assert not res["ok"] and "original DAGs" in res["error"] and af.calls == []


# ------------------------------------------------------------------ existing pipelines
@pytest.fixture
def server(gw):
    m = JobManager(gw.cfg, gw.af)
    srv = make_server(gw.paths, Settings(), 0, jobs=m)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield gw, m, srv.server_address[1]
    srv.shutdown()
    srv.server_close()


def test_orchestrate_route_for_an_existing_pipeline(server):
    gw, m, port = server
    first = m.wait(m.submit(form()), 60)
    assert first["status"] == "succeeded"
    gw.af.paused["customers_daily"] = True                           # someone paused it in Airflow
    con = sqlite3.connect(gw.wh)
    con.execute("DELETE FROM customers_daily_t")
    con.commit()
    con.close()
    gw.af.calls.clear()
    before = tree(gw.repo)
    hist_before = gw.cfg.history.read_text()
    status, _, _ = _req(port, "POST", "/pipeline/customers_daily/orchestrate", {},
                        {"Origin": "http://evil.example"})
    assert status == 403 and gw.af.calls == []
    status, loc, _ = _req(port, "POST", "/pipeline/customers_daily/orchestrate", {})
    assert status == 303 and loc.startswith("/jobs/g-")
    job = m.wait(loc.rsplit("/", 1)[1], 60)
    assert job["kind"] == "orchestrate" and job["status"] == "succeeded", job["error"]
    assert job["rows_loaded"] == 12 and gw.af.paused["customers_daily"] is False
    assert _actions(gw.af) == ["unpause", "trigger"]
    assert tree(gw.repo) == before and gw.cfg.history.read_text() == hist_before   # nothing regenerated
    status, _, html = _req(port, "GET", loc)
    assert status == 200 and "DAG customers_daily is enabled" in html and "Prompt" not in html


def test_orchestrate_failure_pauses_and_keeps_the_existing_table(server):
    gw, m, port = server
    m.wait(m.submit(form()), 60)
    gw.af.outcome = "failed"
    gw.af.paused["customers_daily"] = True
    before = tree(gw.repo)
    status, loc, _ = _req(port, "POST", "/pipeline/customers_daily/orchestrate", {})
    job = m.wait(loc.rsplit("/", 1)[1], 60)
    assert job["status"] == "failed" and gw.af.paused["customers_daily"] is True
    assert "customers_daily_t" in tables(gw.wh) and tree(gw.repo) == before


def test_orchestrate_route_refusals(server):
    gw, m, port = server
    for path, needle in [("/pipeline/stock_counts/orchestrate", "original DAGs"),
                         ("/pipeline/nosuch/orchestrate", "No pipeline")]:
        status, loc, _ = _req(port, "POST", path, {})
        assert status == 303 and "err=1" in loc and needle.replace(" ", "+") in loc, loc
    gw.af.outcome = "hang"
    jid = m.submit(form())
    status, loc, _ = _req(port, "POST", "/pipeline/orders/orchestrate", {})
    assert status == 303 and "err=1" in loc and "job+is+running" in loc
    html = _req(port, "GET", "/pipeline/orders")[2]
    assert "Enable schedule and run now" not in html and "A job is running" in html
    m.cancel(jid)
    m.wait(jid, 30)
    gw.af.available = False
    status, loc, _ = _req(port, "POST", "/pipeline/orders/orchestrate", {})
    assert "Airflow+is+not+running" in loc


# ------------------------------------------------------------------ pages never wait
class SlowAirflow(FakeAirflow):
    def status_all(self):
        time.sleep(1.5)
        return {"orders": {"paused": True, "schedule": "At 00:00", "last_run_state": "success",
                           "last_run_time": "2026-11-28T10:00:00+00:00", "last_run_id": "scheduled__x"}}


def test_pages_never_call_airflow_synchronously(gw):
    af = SlowAirflow(gw.repo, gw.data, gw.wh)
    m = JobManager(gw.cfg, af)
    srv = make_server(gw.paths, Settings(), 0, jobs=m)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    port = srv.server_address[1]
    try:
        t0 = time.monotonic()
        for p in ("/", "/pipelines", "/pipeline/orders", "/incidents", "/governance", "/new", "/jobs"):
            assert _req(port, "GET", p)[0] == 200
        assert time.monotonic() - t0 < 1.4                           # did not wait for the 1.5 s status call
        assert set(af.call_threads) <= {"airflow-status"}            # only the background refresher
        html = _req(port, "GET", "/pipelines")[2]
        assert "checking&hellip;" in html
        end = time.time() + 10
        while time.time() < end and "last run" not in html:
            time.sleep(0.1)
            html = _req(port, "GET", "/pipelines")[2]
        assert ">paused<" in html and "last run" in html
        card = _req(port, "GET", "/pipeline/orders")[2]
        assert "<h2>Airflow</h2>" in card and "2026-11-28 10:00 UTC" in card and "Enable schedule and run now" in card
    finally:
        srv.shutdown()
        srv.server_close()


def test_static_build_has_no_airflow_calls_or_buttons(gw):
    html = route(gw.paths, "/pipeline/orders", {}, live=False)[1]
    assert "shown by the live server" in html and "/orchestrate" not in html
    assert "live only" in route(gw.paths, "/pipelines", {}, live=False)[1]


def test_status_cache_get_returns_immediately():
    c = StatusCache(SlowAirflow(), ttl=60)
    t0 = time.monotonic()
    assert c.get()["dags"] is None and time.monotonic() - t0 < 0.2


def test_loaded_at_shown_in_utc_explicitly():
    assert fmt_utc("2026-11-28T10:58:41Z") == "2026-11-28 10:58 UTC"
    assert fmt_utc("2026-11-28T11:00:47+00:00") == "2026-11-28 11:00 UTC"
    assert fmt_utc("2026-11-28T15:00:00+05:00") == "2026-11-28 10:00 UTC"
    assert fmt_utc("2026-11-28 10:58:41") == "2026-11-28 10:58 UTC"
