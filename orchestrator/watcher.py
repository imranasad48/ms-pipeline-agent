"""Healing orchestrator: poll for new incidents, run the healing agent on each, one at a time.

Selection reuses healing.incidents (open_incidents, incidents_with_proposals). On top of
heal-next's rules the orchestrator adds an allowlist of check-kind prefixes and treats a
proposal in *any* status (incl. FAILED / REJECTED) as "already handled": a human or the
failure record decides what happens next, never an endless automatic retry loop.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import time
from pathlib import Path

import yaml

from healing.incidents import SKIP_PREFIX, incidents_with_proposals, open_incidents
from healing.settings import Settings

PACKAGE_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG = PACKAGE_DIR / "config.yaml"
STATUS_PATH = PACKAGE_DIR / "status.json"      # read by the dashboard banner
ALL_PROPOSAL_STATUSES = ("READY_FOR_APPROVAL", "ESCALATED", "APPLIED", "FAILED", "REJECTED")
DEFAULT_PREFIXES = ("schema.", "value_range.", "duplicates.", "volume.", "nulls.")


def load_config(path: Path | None = None) -> dict:
    cfg = yaml.safe_load(Path(path or DEFAULT_CONFIG).read_text()) or {}
    cfg["auto_heal_prefixes"] = tuple(cfg.get("auto_heal_prefixes") or DEFAULT_PREFIXES)
    cfg["poll_seconds"] = float(os.environ.get("POLL_SECONDS", cfg.get("poll_seconds", 5)))
    return cfg


def is_eligible(inc: dict, prefixes) -> bool:
    """Open warn/critical, check kind on the allowlist, never run_health.*"""
    cid = inc["check_id"]
    return (inc["status"] == "open" and inc["severity"] in ("warn", "critical")
            and not cid.startswith(SKIP_PREFIX) and cid.startswith(tuple(prefixes)))


def pick_eligible(settings: Settings, prefixes, business_date: str | None = None,
                  in_flight: set = frozenset()) -> dict | None:
    """Oldest eligible open incident with no proposal yet (and not currently being healed).
    ``business_date`` narrows the queue to one day (demo mode)."""
    handled = incidents_with_proposals(settings, ALL_PROPOSAL_STATUSES) | set(in_flight)
    for inc in open_incidents(settings, healable_only=True):
        if business_date and inc["business_date"] != business_date:
            continue
        if inc["incident_id"] in handled or not is_eligible(inc, prefixes):
            continue
        return inc
    return None


def _now() -> str:
    return dt.datetime.now().isoformat(timespec="seconds")


def write_status(path: Path, **fields) -> None:
    try:
        tmp = Path(str(path) + ".tmp")
        tmp.write_text(json.dumps({"updated_at": _now(), **fields}))
        os.replace(tmp, path)
    except OSError:
        pass


def log(msg: str) -> None:
    print(f"{dt.datetime.now():%H:%M:%S} [orchestrator] {msg}", flush=True)


class Orchestrator:
    def __init__(self, settings: Settings, cfg: dict, business_date: str | None = None,
                 detect_every: float | None = None, detect_fn=None, heal_fn=None,
                 status_path: Path = STATUS_PATH, sleep=time.sleep, clock=time.monotonic):
        self.s, self.cfg = settings, cfg
        self.business_date, self.detect_every = business_date, detect_every
        self.detect_fn, self.heal_fn = detect_fn, heal_fn
        self.status_path, self.sleep, self.clock = status_path, sleep, clock
        self.in_flight: set = set()
        self.stop = False
        self._last_detect: float | None = None
        self._last_detect_at = ""

    # ---- one cycle -------------------------------------------------------------
    def maybe_detect(self) -> None:
        if not (self.detect_every and self.business_date and self.detect_fn):
            return
        now = self.clock()
        if self._last_detect is not None and now - self._last_detect < self.detect_every:
            return
        self._last_detect = now
        try:
            found = self.detect_fn(self.business_date)
            self._last_detect_at = _now()
            log(f"detection {self.business_date}: {len(found)} incident(s) on record")
        except Exception as e:       # keep watching; the next cycle retries
            log(f"detection failed: {type(e).__name__}: {e}")

    def heal_one(self) -> bool:
        """Heal at most one incident. Returns True if one was attempted."""
        inc = pick_eligible(self.s, self.cfg["auto_heal_prefixes"], self.business_date, self.in_flight)
        if inc is None:
            return False
        iid = inc["incident_id"]
        self.in_flight.add(iid)
        log(f"new incident {iid}: {inc['severity']} {inc['pipeline_id']}/{inc['source_id']} "
            f"{inc['check_id']} ({inc['business_date']}) -> healing")
        write_status(self.status_path, state="healing", incident_id=iid, check_id=inc["check_id"],
                     since=_now(), last_detect=self._last_detect_at)
        try:
            _, verdict = self.heal_fn(self.s, iid)
            log(f"{iid}: verdict {verdict}"
                + ("  -> awaiting human approval on the dashboard" if verdict == "READY_FOR_APPROVAL" else ""))
        except BaseException as e:   # incl. Ctrl-C mid-heal: leave no half-state claim, then re-raise
            log(f"{iid}: heal aborted: {type(e).__name__}: {e}")
            if isinstance(e, (KeyboardInterrupt, SystemExit)):
                raise
        finally:
            self.in_flight.discard(iid)
            write_status(self.status_path, state="idle", last_healed=iid,
                         last_detect=self._last_detect_at)
        return True

    def cycle(self) -> bool:
        self.maybe_detect()
        return self.heal_one()

    # ---- loop --------------------------------------------------------------------
    def run(self, max_cycles: int | None = None) -> None:
        log(f"watching {self.s.incidents_db}  data_root={self.s.data_root}  "
            f"poll={self.cfg['poll_seconds']}s  allow={list(self.cfg['auto_heal_prefixes'])}"
            + (f"  detect every {self.detect_every}s for {self.business_date}" if self.detect_every else "")
            + ("" if not self.business_date else f"  (queue limited to {self.business_date})"))
        write_status(self.status_path, state="idle")
        n = 0
        try:
            while not self.stop and (max_cycles is None or n < max_cycles):
                busy = self.cycle()
                n += 1
                if not busy:
                    self.sleep(self.cfg["poll_seconds"])
        except KeyboardInterrupt:
            pass
        write_status(self.status_path, state="stopped")
        log("stopped")
