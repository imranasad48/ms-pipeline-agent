"""Alert policy: exactly three triggers, at most one email per incident, ever.

  (a) PIPELINE BROKE  - a new CRITICAL incident the orchestrator will not auto-heal
  (b) FIX STAGED      - a heal ends READY_FOR_APPROVAL
  (c) NEEDS A HUMAN   - a heal ends ESCALATED, FAILED_VALIDATION or FAILED

Nothing else mails: warn/info incidents, allowlisted incidents while a heal is pending, applied or
rejected proposals. Backlog guard: on the ledger's first run every incident already in the incident DB
is recorded as 'seeded' and can never alert. No LLM, no network except the optional SMTP send; a mail
failure falls back to the file backend and never raises into the caller.
"""
from __future__ import annotations

import os
import re
from pathlib import Path

from healing.incidents import open_incidents
from healing.proposals import list_proposals, proposal_dir
from healing.settings import Settings

from .ledger import Ledger
from .mailer import PACKAGE_DIR, FileMailer, build_message, select_backend

BROKE, STAGED, HUMAN = "PIPELINE BROKE", "FIX STAGED", "NEEDS A HUMAN"
HEAL_TRIGGERS = {"READY_FOR_APPROVAL": STAGED, "ESCALATED": HUMAN,
                 "FAILED_VALIDATION": HUMAN, "FAILED": HUMAN}
DEFAULT_LEDGER = PACKAGE_DIR / "alerts.db"


def diagnosis_summary(settings: Settings, proposal_id: str, limit: int = 600) -> str:
    """First prose paragraph of the proposal's Diagnosis section (what the agent said went wrong)."""
    try:
        text = (proposal_dir(settings, proposal_id) / "proposal.md").read_text()
    except (OSError, SystemExit):
        return "(no diagnosis available)"
    m = re.search(r"^## Diagnosis\s*\n(.*?)(?=^## |\Z)", text, re.S | re.M)
    for para in re.split(r"\n\s*\n", m.group(1) if m else ""):
        para = " ".join(ln.strip() for ln in para.splitlines()
                        if ln.strip() and not ln.lstrip().startswith("#"))
        if para:
            return para if len(para) <= limit else para[:limit].rsplit(" ", 1)[0] + " ..."
    return "(no diagnosis available)"


class Alerter:
    def __init__(self, settings: Settings, prefixes, mailer=None, ledger_path: Path | None = None,
                 dashboard_url: str | None = None, business_date: str | None = None, env=None,
                 fallback_outbox: Path | None = None):
        env = os.environ if env is None else env
        self.s, self.prefixes, self.business_date = settings, tuple(prefixes), business_date
        self.mailer = mailer or select_backend(env)
        self.dashboard = (dashboard_url or env.get("DASHBOARD_URL") or "http://localhost:8099").rstrip("/")
        self.sender = env.get("ALERT_FROM") or env.get("ALERT_SMTP_USER") or "pipeline-agent@localhost"
        self.ledger = Ledger(ledger_path or DEFAULT_LEDGER)
        self.fallback_outbox = fallback_outbox
        self.warnings: list[str] = []
        if self.ledger.first_run():          # backlog guard: what is on record NOW never alerts
            self.ledger.seed(self._all_incident_ids())

    # ---- helpers ---------------------------------------------------------------
    def _all_incident_ids(self) -> list:
        import sqlite3
        db = Path(self.s.incidents_db)
        if not db.exists():
            return []
        con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
        try:
            return [r[0] for r in con.execute("SELECT incident_id FROM incidents")]
        finally:
            con.close()

    @property
    def recipient(self) -> str:
        return getattr(self.mailer, "recipient", "") or "(outbox file)"

    def describe(self) -> str:
        return f"{self.mailer.describe()}  recipient={self.recipient}"

    def _deliver(self, inc: dict, trigger: str, proposal_id: str | None, body: str) -> bool:
        iid = inc["incident_id"]
        if not self.ledger.claim(iid, trigger, proposal_id, self.recipient):
            return False                      # already mailed (or seeded): at most once, ever
        subject = (f"[pipeline-agent] {trigger}: {inc['pipeline_id']} {inc['check_id']} "
                   f"({inc['business_date']})")
        msg = build_message(subject, body, getattr(self.mailer, "recipient", ""), self.sender)
        outcome, backend = "sent", self.mailer.name
        try:
            where = self.mailer.send(msg)
        except Exception as e:               # never crash or stall the caller
            fallback = FileMailer(self.fallback_outbox or getattr(self.mailer, "outbox", None))
            try:
                where = fallback.send(msg)
                outcome, backend = f"{self.mailer.name}_failed:{type(e).__name__}; written to file", "file"
                self.warnings.append(f"alert {iid}: {self.mailer.name} send failed ({type(e).__name__}); wrote {where}")
            except Exception as e2:
                where, outcome = "", f"failed:{type(e2).__name__}"
        self.ledger.finish(iid, backend, outcome)
        print(f"[alerts] {trigger}: {iid} -> {backend} {where}", flush=True)
        if outcome.startswith(f"{self.mailer.name}_failed"):
            print(f"[alerts] WARNING {self.warnings[-1]}", flush=True)
        return True

    def _base(self, inc: dict) -> str:
        return (f"Pipeline : {inc['pipeline_id']}\nSource   : {inc['source_id']}\n"
                f"Check    : {inc['check_id']}  ({inc['severity']})\n"
                f"Date     : {inc['business_date']}\nIncident : {inc['incident_id']}\n")

    # ---- triggers ----------------------------------------------------------------
    def scan_new_incidents(self) -> int:
        """(a) New critical incidents the orchestrator will not auto-heal."""
        from orchestrator.watcher import is_eligible      # the orchestrator's own allowlist/skip rules
        sent = 0
        for inc in open_incidents(self.s):
            if inc["severity"] != "critical" or is_eligible(inc, self.prefixes):
                continue
            if self.business_date and inc["business_date"] != self.business_date:
                continue
            body = (f"A critical incident was detected that the system will NOT auto-heal.\n\n{self._base(inc)}\n"
                    f"Open the dashboard: {self.dashboard}/\n")
            sent += self._deliver(inc, BROKE, None, body)
        return sent

    def on_heal_result(self, inc: dict, verdict: str) -> bool:
        """(b)/(c) after a heal finishes."""
        trigger = HEAL_TRIGGERS.get(verdict)
        if trigger is None:
            return False
        props = [p for p in list_proposals(self.s) if p.get("incident_id") == inc["incident_id"]]
        pid = max(props, key=lambda p: p.get("updated_at", ""))["proposal_id"] if props else None
        diag = diagnosis_summary(self.s, pid) if pid else "(no proposal recorded)"
        if trigger == STAGED:
            body = (f"A fix is staged and waiting for your approval. Nothing has been applied.\n\n{self._base(inc)}"
                    f"Proposal : {pid}\n\nDiagnosis: {diag}\n\nReview and Approve/Reject: {self.dashboard}/\n")
        else:
            body = (f"The agent could not produce an approvable fix (result: {verdict}). A human is needed.\n\n"
                    f"{self._base(inc)}Proposal : {pid or '-'}\n\nDiagnosis: {diag}\n\n"
                    f"Details: {self.dashboard}/governance\n")
        return self._deliver(inc, trigger, pid, body)
