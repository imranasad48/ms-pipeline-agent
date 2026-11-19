"""Evaluation counters, so `calibrate` can show how close checks came to firing."""
from __future__ import annotations

from collections import defaultdict


class Stats:
    """Counts evaluations/firings per check kind and tracks worst threshold utilisation.

    utilisation = observed / limit; 1.0 means the check was exactly at its trip point.
    """

    def __init__(self):
        self.evaluated = defaultdict(int)
        self.fired = defaultdict(int)
        self.worst = {}

    def evaluate(self, kind: str, n: int = 1) -> None:
        self.evaluated[kind] += n

    def fire(self, check_id: str) -> None:
        self.fired[check_id.split(":", 1)[0]] += 1

    def utilisation(self, kind: str, value: float, limit: float) -> None:
        if limit > 0:
            self.worst[kind] = max(self.worst.get(kind, 0.0), value / limit)
