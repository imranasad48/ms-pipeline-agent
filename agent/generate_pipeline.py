"""Generation Agent v0 — brief in, pipeline + DAG + manifest out.

--data-root (or $PIPELINE_GEN_DATA_ROOT) sets the data the agent inspects and self-tests
against; default ../data_generation/data_clean, i.e. exactly the previous behaviour."""
import argparse, os, anyio
from claude_agent_sdk import query, ClaudeAgentOptions

SYSTEM = """You are the Pipeline Generation Agent inside a self-healing data
engineering system. You build production Airflow pipelines from a plain-language
brief, and you MUST follow the workflow and constraints below exactly.

ENVIRONMENT (current working directory is the pipeline-agent repo):
- Clean source data (build-time reference): ../data_generation/data_clean/
  containing subfolders: crm/ (daily CSVs), 3pl/ (hourly JSON), erp/ (SQLite),
  iot/ (JSONL), manual/ (xlsx).
- Airflow runs in Docker and sees the SAME data at /opt/testbed/data_clean and
  the warehouse at /opt/warehouse/warehouse.db. On this host machine those live
  at ../data_generation/data_clean and ./warehouse/warehouse.db.
- Therefore ALL pipeline code must read paths from env vars with container
  defaults:  DATA_ROOT (default /opt/testbed/data_clean) and
  WAREHOUSE_DB (default /opt/warehouse/warehouse.db).
- DAG files go in ./dags/, importable pipeline logic in ./dags/<pipeline_id>/,
  manifest in ./manifests/<pipeline_id>.yaml.

MANDATORY WORKFLOW:
1. INSPECT: list and sample the relevant source files. Infer the schema
   (columns, types), row counts per file, null rates, key candidates.
2. DERIVE CHECKS first: from the brief + inspection, write down the validation
   assertions the pipeline must satisfy (e.g. all rows loaded, primary key
   unique, required columns non-null).
3. GENERATE: write dags/<pipeline_id>/pipeline.py (pure functions: extract,
   transform, load into sqlite warehouse; full-reload semantics, table replaced
   each run) and dags/<pipeline_id>_dag.py (Airflow DAG, schedule="@daily",
   catchup=False, one task per step calling the pipeline functions).
4. TEST YOURSELF: run the pipeline logic HERE on the host with
   DATA_ROOT=../data_generation/data_clean WAREHOUSE_DB=./warehouse/test_build.db
   using the project venv python (../.venv/bin/python). Then query the output
   db and verify every assertion from step 2. If anything fails, diagnose,
   fix, re-run. Maximum 5 repair attempts, then stop and report failure.
5. MANIFEST: write manifests/<pipeline_id>.yaml with exactly this structure:
   manifest_version, pipeline_id, brief (verbatim), created,
   sources: [ {id, format, path_pattern, expected_schema: {col: type},
               observed: {files, total_rows, null_rates}} ],
   steps: [ {id, file, function, description} ],
   validation_checks: [ {id, type, description, params} ],
   destination: {type, table, path},
   build: {test_db, attempts_used, status}, version: 1.
6. REPORT: print a final line 'FINAL STATUS: SUCCESS' or 'FINAL STATUS: FAILED
   <reason>', plus a 3-line summary of what was built.

HARD CONSTRAINTS:
- Never modify anything under ../data_generation/ (read-only testbed).
- Never touch docker, docker-compose, or install packages.
- Only stdlib + pandas in pipeline code (both exist in the Airflow image).
- Clean up test_build.db at the end.
"""

DEFAULT_DATA_ROOT = "../data_generation/data_clean"


def system_prompt(data_root: str = DEFAULT_DATA_ROOT) -> str:
    """SYSTEM with every host-side data location pointed at data_root (identity for the default)."""
    if data_root == DEFAULT_DATA_ROOT:
        return SYSTEM
    root = data_root.rstrip("/")
    return (SYSTEM.replace(DEFAULT_DATA_ROOT, root)
                  .replace("../data_generation/ (read-only testbed)", f"{root}/ (read-only testbed)"))


async def run(brief: str, data_root: str = DEFAULT_DATA_ROOT):
    opts = ClaudeAgentOptions(
        system_prompt=system_prompt(data_root),
        allowed_tools=["Read", "Write", "Edit", "Bash", "Glob", "Grep"],
        permission_mode="bypassPermissions",
        max_turns=120,
    )
    async for msg in query(prompt=f"BRIEF: {brief}", options=opts):
        for block in getattr(msg, "content", []) or []:
            t = getattr(block, "text", None)
            if t:
                print(t)
            name = getattr(block, "name", None)
            if name:
                inp = str(getattr(block, "input", ""))[:110].replace("\n", " ")
                print(f"  [tool] {name}: {inp}")
        if type(msg).__name__ == "ResultMessage":
            print("\n=== turns:", getattr(msg, "num_turns", "?"),
                  "| cost: subscription ===")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--brief", required=True)
    ap.add_argument("--data-root", default=os.environ.get("PIPELINE_GEN_DATA_ROOT") or DEFAULT_DATA_ROOT,
                    help="data the agent inspects and self-tests against "
                         "(default: $PIPELINE_GEN_DATA_ROOT, else %(default)s)")
    args = ap.parse_args()
    anyio.run(run, args.brief, args.data_root)
