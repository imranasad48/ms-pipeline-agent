"""Alert policy, ledger, backends. Temp dirs/DBs only; SMTP is mocked; nothing touches the network."""
from __future__ import annotations

import json
import smtplib
import sqlite3
import email

import pytest

from alerts.alerter import BROKE, HUMAN, STAGED, Alerter
from alerts.ledger import Ledger
from alerts.mailer import FileMailer, SmtpMailer, select_backend
from healing.case import load_incident
from healing.heal import run_heal
from healing.proposals import update_index
from healing.tests.conftest import _out, good_agent
from orchestrator.watcher import Orchestrator, load_config

PFX = load_config()["auto_heal_prefixes"]
SECRET = "s3cr3t-app-password"


def _add(toy, iid, check, sev="critical", date="2026-08-14", status="open", files=("crm/toy_2026-08-14.csv",)):
    con = sqlite3.connect(toy.db)
    con.execute("INSERT INTO incidents VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (iid, "2026-09-02T00:00:00", date, "toy", "toy_csv", check, "x", sev,
                 "{}", "{}", json.dumps(list(files)), status))
    con.commit(); con.close()


@pytest.fixture
def mk(toy, tmp_path):
    outbox = tmp_path / "outbox"

    def make(**kw):
        return Alerter(toy.settings, PFX, mailer=kw.pop("mailer", FileMailer(outbox)),
                       ledger_path=tmp_path / "alerts.db", fallback_outbox=outbox, env={}, **kw)
    make.outbox = outbox
    return make


def _emls(outbox):
    return sorted(outbox.glob("*.eml")) if outbox.exists() else []


def _read(p):
    return email.message_from_bytes(p.read_bytes())


# ------------------------------------------------------------ triggers
def test_a_unhealable_critical_sends_one_broke_email(toy, mk):
    a = mk()                                   # seeds the toy's two existing incidents
    _add(toy, "inc_new_fresh", "freshness.missing_file")
    _add(toy, "inc_new_rh", "run_health.table_empty")
    assert a.scan_new_incidents() == 2 and a.scan_new_incidents() == 0
    msgs = [_read(p) for p in _emls(mk.outbox)]
    subjects = sorted(m["Subject"] for m in msgs)
    assert subjects == ["[pipeline-agent] PIPELINE BROKE: toy freshness.missing_file (2026-08-14)",
                        "[pipeline-agent] PIPELINE BROKE: toy run_health.table_empty (2026-08-14)"]
    assert "inc_new_fresh" in msgs[0].get_payload() + msgs[1].get_payload()


def _heal(toy, iid, agent):
    case, verdict = run_heal(toy.settings, iid, agent_fn=agent)
    return load_incident(toy.settings, iid), verdict, case


def test_b_ready_for_approval_email_has_diagnosis_proposal_and_link(toy, mk, monkeypatch):
    a = mk(dashboard_url="http://dash.test:9")
    _add(toy, "inc_cccc3333", "schema.renamed:amount->amt", sev="warn")
    inc, verdict, case = _heal(toy, "inc_cccc3333", good_agent)
    assert verdict == "READY_FOR_APPROVAL"
    assert a.on_heal_result(inc, verdict) is True and a.on_heal_result(inc, verdict) is False
    [p] = _emls(mk.outbox)
    m = _read(p)
    body = m.get_payload()
    assert m["Subject"] == "[pipeline-agent] FIX STAGED: toy schema.renamed:amount->amt (2026-08-14)"
    assert case.proposal_id in body and "Vendor renamed amount to amt." in body
    assert "http://dash.test:9/" in body and "toy" in body


def test_c_escalate_failed_validation_and_failed_each_email_once(toy, mk):
    a = mk()

    def escalate(settings, case):
        _out(case, "ESCALATE", "Domain event, not a pipeline defect.")
        return {"ok": True, "error": None}

    def noop(settings, case):
        p = case.staging / "dags/toy/pipeline.py"
        p.write_text(p.read_text() + "\n# nothing\n")
        _out(case, "FIX", "pretend")
        return {"ok": True, "error": None}

    def crash(settings, case):
        return {"ok": False, "error": "boom"}

    for iid, agent, want in [("inc_e1", escalate, "ESCALATED"), ("inc_e2", noop, "FAILED_VALIDATION"),
                             ("inc_e3", crash, "FAILED_VALIDATION")]:
        _add(toy, iid, "schema.renamed:amount->amt", sev="warn")
        inc, verdict, _ = _heal(toy, iid, agent)
        assert verdict == want, (iid, verdict)
        assert a.on_heal_result(inc, verdict) is True
    msgs = [_read(p) for p in _emls(mk.outbox)]
    assert len(msgs) == 3 and all(f"] {HUMAN}: toy" in m["Subject"] for m in msgs)
    assert a.on_heal_result({"incident_id": "inc_e9", "pipeline_id": "toy", "source_id": "s",
                             "check_id": "x", "severity": "warn", "business_date": "d"}, "FAILED") is True


def test_no_email_for_warn_allowlisted_pending_applied_rejected(toy, mk):
    a = mk()
    _add(toy, "inc_w", "freshness.late_file", sev="warn")            # warn: never
    _add(toy, "inc_i", "freshness.late_file", sev="info")            # info: never
    _add(toy, "inc_h", "nulls.spike:amount", sev="critical")         # allowlisted critical, heal pending
    assert a.scan_new_incidents() == 0
    inc = load_incident(toy.settings, "inc_h")
    assert a.on_heal_result(inc, "APPLIED") is False and a.on_heal_result(inc, "REJECTED") is False
    assert _emls(mk.outbox) == []


# ------------------------------------------------------------ ledger
def test_dedupe_survives_restart(toy, mk):
    a = mk()
    _add(toy, "inc_new1", "freshness.missing_file")
    assert a.scan_new_incidents() == 1
    b = mk()                                                          # "restart": new process, same ledger
    assert b.scan_new_incidents() == 0 and len(_emls(mk.outbox)) == 1
    [row] = b.ledger.rows()
    assert (row["incident_id"], row["trigger"], row["outcome"], row["backend"]) == ("inc_new1", BROKE, "sent", "file")
    assert row["sent_at"] and row["recipient"] is not None


def test_one_email_per_incident_across_triggers(toy, mk):
    a = mk()
    _add(toy, "inc_x", "freshness.missing_file")
    a.scan_new_incidents()
    assert a.on_heal_result(load_incident(toy.settings, "inc_x"), "ESCALATED") is False
    assert len(_emls(mk.outbox)) == 1


def test_backlog_guard_seeds_existing_incidents(toy, mk):
    _add(toy, "inc_old_crit", "freshness.missing_file")               # on record before alerting existed
    _add(toy, "inc_old_closed", "run_health.x", status="resolved_pending_verification")
    a = mk()
    assert a.ledger.seeded_count() == 4                               # 2 toy + 2 added
    assert a.scan_new_incidents() == 0 and _emls(mk.outbox) == []
    _add(toy, "inc_after", "freshness.missing_file")                  # first seen after enabling
    assert a.scan_new_incidents() == 1
    assert Ledger(a.ledger.path).seen("inc_old_crit") and mk().scan_new_incidents() == 0
    assert [r["incident_id"] for r in a.ledger.rows()] == ["inc_after"]    # seeded rows hidden by default


def test_business_date_scope_is_respected(toy, mk):
    a = mk(business_date="2026-08-14")
    _add(toy, "inc_other_day", "freshness.missing_file", date="2026-10-01")
    _add(toy, "inc_this_day", "freshness.missing_file")
    assert a.scan_new_incidents() == 1
    assert "inc_this_day" in _read(_emls(mk.outbox)[0]).get_payload()


# ------------------------------------------------------------ backends
def test_smtp_failure_falls_back_to_file_and_never_raises(toy, mk, monkeypatch, capsys):
    class Boom:
        def __init__(self, *a, **k): raise OSError("network unreachable")
    monkeypatch.setattr(smtplib, "SMTP", Boom)
    a = mk(mailer=SmtpMailer("smtp.test", 587, "u@test", SECRET, "to@test"))
    _add(toy, "inc_s", "freshness.missing_file")
    assert a.scan_new_incidents() == 1                                # did not raise
    assert len(_emls(mk.outbox)) == 1
    [row] = a.ledger.rows()
    assert row["backend"] == "file" and row["outcome"].startswith("smtp_failed:OSError")
    out = capsys.readouterr().out
    assert out.count("WARNING") == 1 and SECRET not in out and SECRET not in json.dumps(row)


def test_smtp_success_uses_starttls_587_and_ssl_465(monkeypatch):
    calls = []

    class Fake:
        def __init__(self, host, port, **k): calls.append(("init", port, type(self).__name__))
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def starttls(self, **k): calls.append(("starttls",))
        def login(self, u, p): calls.append(("login", u))
        def send_message(self, m): calls.append(("send", m["To"]))

    class FakeSSL(Fake): pass
    monkeypatch.setattr(smtplib, "SMTP", Fake)
    monkeypatch.setattr(smtplib, "SMTP_SSL", FakeSSL)
    from alerts.mailer import build_message
    m = build_message("s", "b", "to@test", "from@test")
    SmtpMailer("h", 587, "u", SECRET, "to@test").send(m)
    SmtpMailer("h", 465, "u", SECRET, "to@test").send(m)
    assert ("starttls",) in calls and ("init", 465, "FakeSSL") in calls
    assert sum(c[0] == "login" for c in calls) == 2


def test_backend_selection_from_env(tmp_path):
    full = {"ALERT_SMTP_HOST": "h", "ALERT_SMTP_PORT": "587", "ALERT_SMTP_USER": "u@x",
            "ALERT_SMTP_PASSWORD": SECRET, "ALERT_TO": "to@x"}
    m = select_backend(full)
    assert m.name == "smtp" and m.sender == "u@x" and SECRET not in repr(m) and SECRET not in m.describe()
    assert select_backend({**full, "ALERT_FROM": "alerts@x"}).sender == "alerts@x"
    for k in full:                                                    # any one missing -> file
        assert select_backend({kk: v for kk, v in full.items() if kk != k}, tmp_path).name == "file"
    assert select_backend({}, tmp_path).name == "file"
    assert select_backend({**full, "ALERT_SMTP_PORT": "abc"}, tmp_path).name == "file"


def test_file_backend_writes_eml(tmp_path):
    from alerts.mailer import build_message
    p = FileMailer(tmp_path / "ob").send(build_message("[pipeline-agent] TEST: x", "hi", "", "a@b"))
    assert p.endswith(".eml") and _read(__import__("pathlib").Path(p))["Subject"] == "[pipeline-agent] TEST: x"


# ------------------------------------------------------------ orchestrator wiring
def _orch(toy, tmp_path, heal_fn, alerter):
    return Orchestrator(toy.settings, {"auto_heal_prefixes": PFX, "poll_seconds": 0}, heal_fn=heal_fn,
                        alerter=alerter, status_path=tmp_path / "st.json", sleep=lambda s: None)


def test_orchestrator_calls_alerter_scan_and_heal_result(toy, tmp_path, mk):
    a = mk()
    _add(toy, "inc_orch_new", "freshness.missing_file")
    _add(toy, "inc_orch_heal", "schema.renamed:amount->amt", sev="warn", date="2026-01-01")

    def heal(settings, iid):
        update_index(settings, f"p-{iid}", incident_id=iid, status="READY_FOR_APPROVAL")
        return None, "READY_FOR_APPROVAL"

    o = _orch(toy, tmp_path, heal, a)
    o.business_date = None
    assert o.cycle() is True
    subjects = sorted(_read(p)["Subject"] for p in _emls(mk.outbox))
    assert any("PIPELINE BROKE: toy freshness.missing_file" in s for s in subjects)
    assert any("FIX STAGED: toy schema.renamed:amount->amt (2026-08-14)" in s for s in subjects) \
        or any("FIX STAGED: toy schema.renamed:amount->amt (2026-01-01)" in s for s in subjects)
    assert len(subjects) == 2


def test_alerter_exceptions_never_break_the_loop(toy, tmp_path):
    class Bad:
        def scan_new_incidents(self): raise RuntimeError("alerts down")
        def on_heal_result(self, inc, verdict): raise RuntimeError("alerts down")

    healed = []

    def heal(settings, iid):
        healed.append(iid)
        update_index(settings, f"p-{iid}", incident_id=iid, status="READY_FOR_APPROVAL")
        return None, "READY_FOR_APPROVAL"

    o = _orch(toy, tmp_path, heal, Bad())
    assert o.cycle() is True and healed                               # heal ran, loop survived
    assert o.in_flight == set()


def test_no_alerter_is_a_noop(toy, tmp_path):
    o = _orch(toy, tmp_path, lambda s, i: (None, "FAILED"), None)
    assert o.heal_one() is True
