"""Detection arm: replay the watcher over a scenario's full date range (deterministic, 1 run)."""
from __future__ import annotations

import re
import subprocess
from pathlib import Path

from . import settings as S
from .builder import scenario_dir


def date_range_of(data_root: Path) -> tuple[str, str]:
    dates = {m.group(1) for p in Path(data_root).rglob("*") if (m := re.search(r"(\d{4}-\d{2}-\d{2})", p.name))}
    if not dates:
        raise SystemExit(f"no dated files under {data_root}")
    return min(dates), max(dates)


def run_detection(failure_type: str, seed: int, root: Path | None = None) -> Path:
    sdir = scenario_dir(failure_type, seed, root)
    if not (sdir / "data").is_dir():
        raise SystemExit(f"scenario {failure_type}/{seed} not built; run `build` first")
    db = sdir / "incidents.db"
    db.unlink(missing_ok=True)                      # fresh store -> reruns identical
    lo, hi = date_range_of(sdir / "data")
    proc = subprocess.run(
        [S.PYTHON, "-m", "detection", "replay", "--from", lo, "--to", hi,
         "--data-root", str(sdir / "data"), "--incidents-db", str(db)],
        cwd=S.REPO_ROOT, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"detection replay failed for {failure_type}:\n{proc.stderr[-1500:]}")
    print(f"[detect] {failure_type}/{seed}: {lo}..{hi} -> {db}")
    return db
