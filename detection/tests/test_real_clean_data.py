"""Integration smoke test against the real clean testbed (skipped if not present)."""
import datetime as dt
from pathlib import Path

import pytest

from detection import config as C
from detection.checks.context import Context
from detection.engine import run_date
from detection.manifest import load_manifests

CLEAN = C.REPO_ROOT.parent / "data_generation" / "data_clean"


@pytest.mark.skipif(not CLEAN.exists() or not C.DEFAULT_MANIFESTS.exists(),
                    reason="clean testbed not available")
@pytest.mark.parametrize("day", ["2026-08-14", "2026-09-30", "2026-11-11"])
def test_clean_days_have_zero_incidents(day):
    cfg = C.load_config()
    ctx = Context(CLEAN, C.DEFAULT_WAREHOUSE, cfg)
    assert run_date(load_manifests(C.DEFAULT_MANIFESTS, cfg), dt.date.fromisoformat(day), ctx) == []
