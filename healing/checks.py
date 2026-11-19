"""Steps 2 & 4 -- run a pipeline package + detection and return flat check results."""
from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

from detection import config as dcfg
from detection.checks.context import Context
from detection.engine import run_date
from detection.manifest import load_manifest

from .settings import RUNNER, Settings
from .verdict import FAIL, PASS


def runner_argv(settings: Settings, pipeline_root: Path, pid: str, steps: list,
                data_root: Path, work: Path, tag: str, out: Path) -> list:
    """The exact command used by baseline, validation AND the agent's self-test."""
    return [settings.python, str(RUNNER), "--pipeline-root", str(pipeline_root), "--pid", pid,
            "--steps", ",".join(steps), "--data-root", str(data_root),
            "--warehouse", str(work / f"{tag}_warehouse.db"),
            "--staging-dir", str(work / f"{tag}_stage"), "--out", str(out)]


def run_pipeline(settings: Settings, pipeline_root: Path, pid: str, steps: list,
                 data_root: Path, work: Path, tag: str) -> dict:
    """Run the pipeline in a subprocess into a throw-away warehouse."""
    work.mkdir(parents=True, exist_ok=True)
    out = work / f"{tag}_result.json"
    out.unlink(missing_ok=True)
    for stale in (work / f"{tag}_warehouse.db",):
        stale.unlink(missing_ok=True)
    env = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}
    proc = subprocess.run(runner_argv(settings, pipeline_root, pid, steps, data_root, work, tag, out),
                          capture_output=True, text=True, env=env, timeout=900)
    if not out.exists():
        return {"results": {f"step:{s}": FAIL for s in steps},
                "errors": {"runner": (proc.stderr or proc.stdout)[-800:]}}
    return json.loads(out.read_text())


def detection_failures(settings: Settings, manifest_path: Path, business_date) -> set:
    """Detection check ids ('<source>:<check>') firing for this date, source-level only."""
    cfg = dcfg.load_config()
    ctx = Context(settings.data_root, settings.warehouse_db, cfg)
    m = load_manifest(manifest_path, cfg)
    return {f"{i.source_id}:{i.check_id}" for i in run_date([m], business_date, ctx)
            if i.check_family != "run_health"}
