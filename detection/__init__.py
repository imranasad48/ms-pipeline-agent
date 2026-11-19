"""Rule-based data-pipeline watcher.

Deterministic Python only -- no LLM calls anywhere in this package.
Compiles each pipeline manifest into checks (schema, volume/nulls,
freshness, run health), runs them for one business date and records
the failures as rows in ``incidents.db``.
"""
