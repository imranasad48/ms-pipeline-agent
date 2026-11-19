"""Load detection thresholds (config.yaml) and learned baselines (baselines.yaml)."""
from __future__ import annotations

from pathlib import Path

import yaml

PACKAGE_DIR = Path(__file__).resolve().parent
REPO_ROOT = PACKAGE_DIR.parent
DEFAULT_CONFIG = PACKAGE_DIR / "config.yaml"
DEFAULT_BASELINES = PACKAGE_DIR / "baselines.yaml"
DEFAULT_MANIFESTS = REPO_ROOT / "manifests"
DEFAULT_WAREHOUSE = REPO_ROOT / "warehouse" / "warehouse.db"
DEFAULT_INCIDENTS_DB = PACKAGE_DIR / "incidents.db"


def load_config(path: Path | None = None, baselines: Path | None = None) -> dict:
    """Return the merged config dict; baselines land under ``cfg['baselines']``."""
    cfg = yaml.safe_load(Path(path or DEFAULT_CONFIG).read_text()) or {}
    bpath = Path(baselines or DEFAULT_BASELINES)
    cfg["baselines"] = (yaml.safe_load(bpath.read_text()) or {}) if bpath.exists() else {}
    return cfg


def resolve_severity(cfg: dict, check_id: str, is_key: bool = False) -> str:
    """Look up severity for a check id (the part before ':' selects the rule)."""
    rule = cfg["severity"].get(check_id.split(":", 1)[0], "warn")
    if isinstance(rule, dict):
        return rule["key" if is_key else "other"]
    return rule
