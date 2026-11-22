"""Shared run context handed to every check."""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from pathlib import Path

from ..stats import Stats


@dataclass
class Context:
    data_root: Path
    warehouse_db: Path
    config: dict
    now: dt.datetime = field(default_factory=lambda: dt.datetime.now(dt.timezone.utc))
    stats: Stats = field(default_factory=Stats)
    cache: dict = field(default_factory=dict)   # profiles / key sets reused across days
