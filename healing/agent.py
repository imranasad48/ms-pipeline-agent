"""Step 3 -- the LLM agent session (claude_agent_sdk). The ONLY LLM call in healing.

The agent works inside proposals/<id>/staging/ on COPIES of the pipeline package.
"""
from __future__ import annotations

import json
from pathlib import Path

from claude_agent_sdk import ClaudeAgentOptions, query

from .case import Case
from .checks import runner_argv
from .settings import Settings

SYSTEM = """You are the Healing Agent inside a self-healing data-engineering system.
A deterministic detector raised an incident on a data pipeline. You diagnose it
and, if it is a plumbing defect, patch STAGED COPIES of the pipeline so a human can
review and approve the change.

ABSOLUTE RULES
- NEVER read, open, list, grep, copy or reference ../data_generation/data/error_manifest.json
  or error_manifest.xlsx (anywhere). It is the evaluation answer key; reading it
  invalidates the research. If you find yourself wanting ground truth, work only from
  the case files.
- Your working directory is the STAGING directory. Edit ONLY files under it:
  dags/<pipeline>/pipeline.py and (only if the contract itself must change)
  manifests/<pipeline>.yaml. NEVER write anywhere else: not the live repo's dags/ or
  manifests/, not ../data_generation, not the warehouse, not detection/.
- Do not change anything under case/. Do not install packages. No git commands.
- Never weaken or delete the pipeline's own validation checks to make a run pass.

CASE FILES (all under ./case/)
- incident.json: the detection incident (check_id, expected/observed evidence, files).
- baseline.json: what happened when the UNPATCHED pipeline ran on the case data
  (per-check pass/fail/skipped and error messages).
- raw_files/: copies of the affected raw file(s). data_root/: the case DATA_ROOT the
  pipeline runs against (the incident date's files; other incidents' files removed).
- scope.json explains which files are in data_root. baseline.json also has `control`:
  results of the unpatched pipeline with THIS incident's files removed; anything failing
  there (`scope_artifacts`) is caused by the partial data slice, NOT by this incident or
  your patch -- do not try to fix it and do not alter validation checks for it.

WORKFLOW
1. DIAGNOSE: read incident.json, baseline.json, the pipeline code and the raw file(s).
   Find the root cause in plain language.
2. CLASSIFY exactly one of:
   FIX      - a data-plumbing defect the code/contract can absorb safely: schema drift
              (rename, new/dropped column), type change/format drift, parse issue,
              duplicates, ordering. The data's MEANING is unchanged.
   ESCALATE - a plausible real-world event (e.g. genuinely alarming sensor values,
              a real business anomaly) or anything where the right behaviour is
              ambiguous or needs a business decision. Patching would hide a signal.
   If ESCALATE: write agent_out/diagnosis.md (what you saw, why it needs a human,
   recommendation), write agent_out/decision.json, change NO other file, and STOP.
3. If FIX: make the smallest robust patch to the staged pipeline.py. Prefer accepting
   both the old and new form (e.g. map the renamed key back to the canonical name before
   the existing schema check) over breaking the old behaviour. Only edit the staged
   manifest if the contract genuinely changes; if you do, increment its `version`
   integer and append an entry (string describing the change) to a top-level
   `change_log:` list in that yaml.
4. SELF-TEST (max {max_attempts} attempts): run exactly this command from the staging dir
   and read its output; iterate until all steps and validate checks pass:
   {selftest}
   Results are written to agent_out/selftest.json (key `results`: step:* / validate:*).
5. WRITE OUTPUTS:
   - agent_out/diagnosis.md : root cause, evidence, classification reasoning, what you
     changed and why (plain language, a human reads it in a minute).
   - agent_out/decision.json : {{"decision": "FIX"|"ESCALATE", "root_cause": "<one sentence>",
     "attempts": <int>, "files_changed": ["dags/.../pipeline.py", ...]}}
6. Final line of your reply: `DECISION: FIX` or `DECISION: ESCALATE`.
"""


def selftest_command(settings: Settings, case: Case) -> str:
    argv = runner_argv(settings, Path("."), case.pipeline_id, case.steps, case.scoped_root,
                       case.work / "agent", "agent", Path("agent_out/selftest.json"))
    return " ".join(f'"{a}"' if " " in a else a for a in argv)


def build_prompts(settings: Settings, case: Case) -> tuple[str, str]:
    system = SYSTEM.format(max_attempts=settings.max_attempts,
                           selftest=selftest_command(settings, case))
    user = (f"Incident {case.incident['incident_id']} on pipeline {case.pipeline_id} "
            f"(business date {case.incident['business_date']}): "
            f"{case.incident['check_id']}. Start by reading case/incident.json and "
            f"case/baseline.json. Staged files you may edit: {', '.join(case.live_files)}.")
    return system, user


async def run_agent(settings: Settings, case: Case) -> dict:
    """Run the SDK session; returns {'ok': bool, 'error': str|None, 'transcript': [...]}."""
    system, user = build_prompts(settings, case)
    opts = ClaudeAgentOptions(
        system_prompt=system, cwd=str(case.staging),
        allowed_tools=["Read", "Write", "Edit", "Bash", "Glob", "Grep"],
        permission_mode="bypassPermissions", max_turns=80,
        env={"PYTHONDONTWRITEBYTECODE": "1"})
    log: list = []
    try:
        async for msg in query(prompt=user, options=opts):
            for block in getattr(msg, "content", []) or []:
                text = getattr(block, "text", None)
                if text:
                    print(text)
                    log.append(text)
                if getattr(block, "name", None):
                    line = f"  [tool] {block.name}: {str(getattr(block, 'input', ''))[:110]}"
                    print(line.replace("\n", " "))
                    log.append(line)
    except Exception as e:  # SDK/auth/network failure -> recorded, verdict FAILED
        return {"ok": False, "error": f"{type(e).__name__}: {e}", "transcript": log}
    (case.root / "agent_transcript.txt").write_text("\n".join(log))
    return {"ok": True, "error": None, "transcript": log}


def read_decision(case: Case) -> dict | None:
    p = case.staging / "agent_out" / "decision.json"
    try:
        return json.loads(p.read_text())
    except (OSError, ValueError):
        return None
