"""Paths and knobs for one healing session (injectable so tests can use a toy repo)."""
from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from pathlib import Path

PACKAGE_DIR = Path(__file__).resolve().parent
REPO_ROOT = PACKAGE_DIR.parent
RUNNER = PACKAGE_DIR / "_runner.py"


@dataclass
class Settings:
    repo_root: Path = REPO_ROOT
    data_root: Path = field(default_factory=lambda: Path(
        os.environ.get("HEAL_DATA_ROOT", REPO_ROOT.parent / "data_generation" / "data")))
    incidents_db: Path = REPO_ROOT / "detection" / "incidents.db"
    proposals_dir: Path = PACKAGE_DIR / "proposals"
    warehouse_db: Path = REPO_ROOT / "warehouse" / "warehouse.db"   # live; read-only here
    python: str = sys.executable
    max_attempts: int = 4

    @property
    def index_path(self) -> Path:
        return self.proposals_dir / "index.json"
