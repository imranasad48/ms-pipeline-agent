"""Paths and constants. The generator dir is READ-ONLY: it is only ever copied, never edited."""
from __future__ import annotations

import sys
from pathlib import Path

PACKAGE_DIR = Path(__file__).resolve().parent
REPO_ROOT = PACKAGE_DIR.parent
GENERATOR_DIR = REPO_ROOT.parent / "data_generation"
SCENARIOS_DIR = PACKAGE_DIR / "scenarios"
RESULTS_DIR = PACKAGE_DIR / "results"
MAPPING_PATH = PACKAGE_DIR / "mapping.yaml"
PYTHON = sys.executable

# The generator's base config says `end_date: today`, which makes output depend on the
# day it is run. Scenarios pin the window to the one the manifests/baselines were built on.
DEFAULT_END_DATE = "2026-11-11"
DEFAULT_SEED = 20260904        # same base seed as ../data_generation/config.yaml

# Generator source needed to build a scenario (everything else -- data, review pack, zips -- is skipped).
GENERATOR_SOURCE = ("generate.py", "pharma_synth", "config.yaml")
