"""Scenario builder: one failure type per scenario, generated from a scratch COPY of the generator.

../data_generation is never modified: its source is copied to a scratch dir, a config with
ONLY the requested injector enabled is written there, generate.py runs there, and the dirty
data is harvested into evaluation/scenarios/<type>/<seed>/.

Layout of a scenario:
    data/                       dirty data root (what detection / healing see)
    ground_truth/error_manifest.json   answer key, deliberately OUTSIDE data/ so a healing
                                       agent browsing its data root cannot stumble on it
    config.yaml                 the exact generator config used
    meta.json                   seed, end_date, failure type, build time
"""
from __future__ import annotations

import copy
import datetime as dt
import json
import shutil
import subprocess
import tempfile
from pathlib import Path

import yaml

from . import settings as S


def base_config() -> dict:
    return yaml.safe_load((S.GENERATOR_DIR / "config.yaml").read_text())


def all_failure_types(base: dict | None = None) -> list[str]:
    return list((base or base_config())["failures"])


def resolve_types(spec: str, base: dict | None = None) -> list[str]:
    known = all_failure_types(base)
    if spec.strip() == "all":
        return known
    types = [t.strip() for t in spec.split(",") if t.strip()]
    bad = [t for t in types if t not in known]
    if bad:
        raise SystemExit(f"unknown failure type(s): {', '.join(bad)}\nknown: {', '.join(known)}")
    return types


def make_config(base: dict, failure_type: str, seed: int, end_date: str) -> dict:
    """Pure function: base config -> config with ONLY `failure_type` enabled."""
    if failure_type not in base["failures"]:
        raise ValueError(f"unknown failure type {failure_type!r}")
    cfg = copy.deepcopy(base)
    cfg["seed"] = int(seed)
    cfg["end_date"] = end_date
    cfg["guarantee_min_occurrences"] = True       # every scenario holds >=1 injection
    cfg["output"]["clean_baseline"] = False       # scenarios need only the dirty run
    for key, spec in cfg["failures"].items():
        spec["enabled"] = key == failure_type
    return cfg


def scenario_dir(failure_type: str, seed: int, root: Path | None = None) -> Path:
    return (root or S.SCENARIOS_DIR) / failure_type / str(seed)


def build_scenario(failure_type: str, seed: int = S.DEFAULT_SEED,
                   end_date: str = S.DEFAULT_END_DATE, root: Path | None = None,
                   force: bool = False) -> Path:
    dest = scenario_dir(failure_type, seed, root)
    if (dest / "meta.json").exists() and not force:
        print(f"[build] {failure_type}/{seed}: exists, skipping (use --force to rebuild)")
        return dest
    cfg = make_config(base_config(), failure_type, seed, end_date)
    with tempfile.TemporaryDirectory(prefix=f"evalbuild_{failure_type}_") as tmp:
        scratch = Path(tmp)
        for name in S.GENERATOR_SOURCE:
            src = S.GENERATOR_DIR / name
            if src.is_dir():
                shutil.copytree(src, scratch / name,
                                ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
            else:
                shutil.copy2(src, scratch / name)
        (scratch / "config.yaml").write_text(yaml.safe_dump(cfg, sort_keys=False))
        t0 = dt.datetime.now()
        proc = subprocess.run([S.PYTHON, "generate.py", "--no-clean", "--no-review-pack"],
                              cwd=scratch, capture_output=True, text=True)
        if proc.returncode != 0:
            raise RuntimeError(f"generator failed for {failure_type}:\n{proc.stdout[-800:]}\n{proc.stderr[-1500:]}")
        data = scratch / "data"
        if dest.exists():
            shutil.rmtree(dest)
        (dest / "ground_truth").mkdir(parents=True)
        shutil.move(str(data / "error_manifest.json"), dest / "ground_truth" / "error_manifest.json")
        (data / "error_manifest.xlsx").unlink(missing_ok=True)
        shutil.move(str(data), dest / "data")
        (dest / "config.yaml").write_text(yaml.safe_dump(cfg, sort_keys=False))
        (dest / "meta.json").write_text(json.dumps({
            "failure_type": failure_type, "seed": seed, "end_date": end_date,
            "history_days": cfg["history_days"], "built_at": dt.datetime.now().isoformat(timespec="seconds"),
            "build_seconds": round((dt.datetime.now() - t0).total_seconds(), 1)}, indent=1))
    # scratch dir is removed by the context manager
    print(f"[build] {failure_type}/{seed}: {dir_mb(dest):.0f} MB -> {dest}")
    return dest


def dir_mb(path: Path) -> float:
    return sum(p.stat().st_size for p in Path(path).rglob("*") if p.is_file()) / 1e6
