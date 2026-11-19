"""Pure verdict logic: compare before/after check results. No I/O, no LLM."""
from __future__ import annotations

READY = "READY_FOR_APPROVAL"
FAILED = "FAILED_VALIDATION"
ESCALATED = "ESCALATED"
PASS, FAIL, SKIPPED = "pass", "fail", "skipped"


def select_targets(before: dict, incident_key: str | None) -> list:
    """Which checks must turn green for the incident to count as resolved.

    The pipeline-level checks that *failed* at baseline (step:/validate:). If the
    pipeline itself was fine (e.g. a null spike it tolerates), fall back to the
    detection check that raised the incident.
    """
    failing = sorted(k for k, v in before.items()
                     if v == FAIL and not k.startswith("detect:"))
    if failing:
        return failing
    return [incident_key] if incident_key and incident_key in before else []


def decide(before: dict, after: dict, target_keys: list, scope_artifacts=()) -> dict:
    """Verdict rules (all must hold for READY_FOR_APPROVAL):

    1. every target check passes after the patch;
    2. no check that passed at baseline is non-passing now (regression);
    3. no check that was skipped (or never ran) at baseline now fails (a masked failure surfaced),
       unless it also fails in the CONTROL run (same scope, incident files removed):
       such "scope artifacts" come from the case data being a partial slice, not the patch;
    4. there is at least one target (otherwise there is nothing to verify).
    """
    unresolved = [k for k in target_keys if after.get(k) != PASS]
    regressions = sorted(k for k, v in before.items()
                         if v == PASS and after.get(k, FAIL) != PASS)
    artifacts = set(scope_artifacts)
    unmasked = sorted(k for k, v in before.items()
                      if v == SKIPPED and after.get(k) == FAIL and k not in artifacts)
    # checks that never ran at baseline (e.g. validate:* behind a failed step) count too
    unmasked = sorted(set(unmasked) | {k for k, v in after.items()
                                       if v == FAIL and k not in before and k not in artifacts})
    ok = bool(target_keys) and not unresolved and not regressions and not unmasked
    return {"verdict": READY if ok else FAILED, "target_keys": list(target_keys),
            "target_resolved": bool(target_keys) and not unresolved,
            "unresolved_targets": unresolved, "regressions": regressions,
            "unmasked_failures": unmasked,
            "scope_artifacts": sorted(k for k in artifacts if after.get(k) == FAIL)}


def merge_detection(before: dict, after_failing: set, before_failing: set) -> tuple[dict, dict]:
    """Fold detection findings into before/after dicts (firing = fail, silent = pass)."""
    universe = before_failing | after_failing
    b = {f"detect:{k}": FAIL if k in before_failing else PASS for k in universe}
    a = {f"detect:{k}": FAIL if k in after_failing else PASS for k in universe}
    return b, a


def table_rows(before: dict, after: dict, target_keys: list, scope_artifacts=()) -> list:
    """(check, before, after, note) rows, sorted for display."""
    rows = []
    for k in sorted(set(before) | set(after)):
        b, a = before.get(k, "-"), after.get(k, "-")
        note = "TARGET" if k in target_keys else ""
        if b == PASS and a != PASS:
            note = "REGRESSION"
        elif a == FAIL and k in scope_artifacts:
            note = "scope artifact (also fails without the incident's files)"
        elif a == FAIL and b in (SKIPPED, "-"):
            note = "UNMASKED FAILURE"
        elif k.startswith("detect:") and b == a == FAIL:
            note = "source file still dirty (expected: code-only fix)"
        elif b == FAIL and a == PASS and not note:
            note = "fixed"
        rows.append((k, b, a, note))
    return rows
