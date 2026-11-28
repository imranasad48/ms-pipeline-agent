"""Pipeline generation jobs started from the dashboard (POST /generate).

One job at a time. A job:
  1. snapshots dags/ and manifests/ (copies + hashes, in the job dir),
  2. runs the generation agent as a background subprocess in its own process group, with an
     argument list (never a shell; the user's prompt is one argv element), streaming its output
     into log.txt,
  3. checks what it produced (FINAL STATUS line, the three expected files, the manifest, and that
     nothing else under dags/ or manifests/ was created or changed),
  4. runs it through Airflow ("orchestrating", see airflow.orchestrate): waits until Airflow has
     parsed the DAG, refuses catchup=True, unpauses it (from then on it runs on its schedule),
     triggers one run `dashboard__<job_id>` and waits for it; the run writes the warehouse,
  5. appends one line to agent/generation_history.jsonl.
Any unsuccessful end (failure, cancel, time limit, server restart) pauses the DAG again, drops the
table the run created, and restores dags/ and manifests/ to the snapshot; whatever the job wrote
there is moved into <job>/quarantine/.

Preflight at submit (before any agent usage): Airflow must be healthy, the warehouse must be the
file Airflow writes, and every selected source must be byte-identical between the job's data root
(what the agent inspects) and the folder Airflow reads (../data_generation/data_clean, mounted at
/opt/testbed/data_clean); otherwise the job is refused.

"Enable schedule and run now" for an existing pipeline (submit_orchestrate) runs only step 4, as a
tracked job; on failure its DAG is paused again and nothing else is touched.

Auth: the subprocess inherits the server's environment (the Claude subscription login). No API
key is read, set or required here. Files named error_manifest.* are never listed, and log lines
mentioning them are withheld.
"""
from __future__ import annotations

import datetime as dt
import filecmp
import getpass
import hashlib
import json
import os
import re
import secrets
import shutil
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from . import airflow as afl
from .catalog import build_catalog
from .data import read_manifest

REPO = Path(__file__).resolve().parent.parent
RUNNER = REPO / "healing" / "_runner.py"

ACTIVE = ("queued", "generating", "loading", "orchestrating")
TERMINAL = ("succeeded", "failed", "cancelled", "interrupted")
ID_RE = re.compile(r"^[a-z][a-z0-9_]{1,47}$")
JOB_ID_RE = re.compile(r"^g-\d{8}-\d{6}-[0-9a-f]{4}$")
TABLE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
PROMPT_MAX = 4000
MAX_SOURCES = 10
# ids that would shadow an importable module once dags/ is on sys.path (Airflow does that)
RESERVED = set(getattr(sys, "stdlib_module_names", ())) | {
    "airflow", "pandas", "numpy", "yaml", "pytest", "dags", "agent", "detection", "healing",
    "orchestrator", "alerts", "dashboard", "evaluation", "pipeline", "tests", "test", "smoke_test"}
WATCHED = ("dags", "manifests")


def _default_limit() -> float:
    try:
        return float(os.environ.get("PIPELINE_GEN_TIME_LIMIT_MIN", "40")) * 60
    except ValueError:
        return 40 * 60


@dataclass
class GenConfig:
    repo_root: Path = REPO                       # cwd of the agent; it writes ./dags and ./manifests
    data_root: Path = field(default_factory=lambda: Path(
        os.environ.get("HEAL_DATA_ROOT", REPO.parent / "data_generation" / "data")))
    warehouse: Path = field(default_factory=lambda: Path(
        os.environ.get("WAREHOUSE_DB", REPO / "warehouse" / "warehouse.db")))
    jobs_dir: Path = REPO / "dashboard" / "jobs"
    history: Path = REPO / "agent" / "generation_history.jsonl"
    generator: list | None = None                # argv prefix; "--data-root <dir> --brief <text>" appended
    python: str = sys.executable
    time_limit_s: float = field(default_factory=_default_limit)
    kill_grace_s: float = 5.0
    # what Airflow reads and writes, as host paths (docker-compose.yaml mounts)
    airflow_data: Path = field(default_factory=lambda: Path(
        os.environ.get("PIPELINE_AIRFLOW_DATA", REPO.parent / "data_generation" / "data_clean")))
    airflow_data_container: str = afl.AirflowCLI.data_dir_container
    airflow_warehouse: Path = REPO / "warehouse" / "warehouse.db"
    airflow_poll_s: float = 5.0

    @property
    def manifests(self) -> Path:
        return Path(self.repo_root) / "manifests"

    @property
    def dags(self) -> Path:
        return Path(self.repo_root) / "dags"

    def generator_argv(self) -> list:
        return list(self.generator or [self.python, str(REPO / "agent" / "generate_pipeline.py")])

    def job_argv(self, brief: str) -> list:
        """The full generator command: the agent inspects and self-tests against the SAME data
        root the load uses, never its built-in default under ../data_generation."""
        return self.generator_argv() + ["--data-root", str(Path(self.data_root).resolve()), "--brief", brief]


def now_iso() -> str:
    return dt.datetime.now().isoformat(timespec="seconds")


def actor() -> str:
    try:
        return os.environ.get("PIPELINE_AGENT_USER") or getpass.getuser()
    except Exception:
        return "unknown"


def _withhold(line: str) -> str:
    return ("[dashboard] (line withheld: it refers to the evaluation ground-truth file)\n"
            if "error_manifest" in line.lower() else line)


# ------------------------------------------------------------------ guards
def answer_key_present(data_root: Path) -> bool:
    """True if any file named error_manifest.* exists beneath data_root. Names only: nothing is
    opened, and the location is never reported."""
    for _dir, _dirs, files in os.walk(Path(data_root)):
        if any(f.startswith("error_manifest.") for f in files):
            return True
    return False


ANSWER_KEY_REFUSAL = ("The data root contains an evaluation answer key (a file named error_manifest.*). "
                      "Generation is refused so the agent can never read it: point HEAL_DATA_ROOT at a data "
                      "root without it (e.g. demo_root) and restart the dashboard.")
GENERATING_REFUSAL = "A pipeline is being generated; approve after it finishes."


def active_job_on_disk(jobs_dir: Path | None = None) -> dict | None:
    """An active generation job recorded under jobs_dir, from any process. A job whose recorded
    process has died is not counted (the next dashboard start marks it interrupted)."""
    for jf in sorted(Path(jobs_dir or GenConfig.jobs_dir).glob("g-*/job.json"), reverse=True):
        try:
            job = json.loads(jf.read_text())
        except (OSError, ValueError):
            continue
        if job.get("status") in ACTIVE:
            pid = job.get("pid")
            if pid:
                try:
                    os.kill(pid, 0)
                except OSError:
                    continue
            return job
    return None


def refuse_if_generating(jobs_dir: Path | None = None) -> None:
    """For `python -m healing apply`: refuse while a dashboard generation job is active, because a
    failed job restores dags/ and manifests/ and would undo the applied fix."""
    job = active_job_on_disk(jobs_dir)
    if job:
        raise SystemExit(f"{GENERATING_REFUSAL} (job {job['job_id']}, pipeline {job['pipeline_id']}; "
                         "see the dashboard's Jobs page)")


# ------------------------------------------------------------------ validation
def existing_pipeline_ids(cfg: GenConfig) -> set:
    ids = set()
    for p in cfg.manifests.glob("*.y*ml") if cfg.manifests.is_dir() else []:
        ids.add(p.stem)
        m = read_manifest(p)
        if m:
            ids.add(str(m["pipeline_id"]))
    if cfg.dags.is_dir():
        for p in cfg.dags.iterdir():
            ids.add(p.name[:-7] if p.name.endswith("_dag.py") else p.stem if p.suffix == ".py" else p.name)
    return ids - {""}


def id_problem(cfg: GenConfig, pid: str) -> str:
    if not ID_RE.match(pid):
        return ("Pipeline id must be 2-48 characters: lowercase letters, digits and underscores, "
                "starting with a letter.")
    if pid in RESERVED:
        return f"Pipeline id “{pid}” would shadow a Python module; choose another."
    if pid in existing_pipeline_ids(cfg):
        return f"A pipeline “{pid}” already exists in manifests/ or dags/; it will not be overwritten."
    return ""


def validate_request(cfg: GenConfig, form: dict) -> tuple[dict, list[str]]:
    """form is parse_qs-shaped. Returns (clean request, errors)."""
    errors = []
    if answer_key_present(cfg.data_root):
        errors.append(ANSWER_KEY_REFUSAL)
    catalog = {s["key"]: s for s in build_catalog(cfg.data_root, cfg.manifests)}
    raw = [s for s in (form.get("source") or []) if s]
    sources = list(dict.fromkeys(raw))
    if not sources:
        errors.append("Select at least one source.")
    elif len(sources) > MAX_SOURCES:
        errors.append(f"Select at most {MAX_SOURCES} sources.")
    unknown = [s for s in sources if s not in catalog]
    if unknown:
        errors.append("Unknown source(s); pick from the catalog: " + ", ".join(repr(s[:80]) for s in unknown[:5]))
    pid = ((form.get("pipeline_id") or [""])[0]).strip()
    p = id_problem(cfg, pid)
    if p:
        errors.append(p)
    prompt = ((form.get("prompt") or [""])[0]).replace("\r\n", "\n").strip()
    if not prompt:
        errors.append("Describe what the pipeline should produce.")
    elif len(prompt) > PROMPT_MAX:
        errors.append(f"The prompt is {len(prompt)} characters; the limit is {PROMPT_MAX}.")
    elif "\x00" in prompt:
        errors.append("The prompt contains a NUL character.")
    picked = [catalog[s] for s in sources if s in catalog]
    errors += preflight_problems(cfg, picked)
    return {"sources": sources, "pipeline_id": pid, "prompt": prompt,
            "covered": sorted({x for s in picked for x in s["covered_by"]}),
            "source_info": picked}, errors


def data_differences(cfg: GenConfig, sources: list[dict]) -> list[str]:
    """Files of the selected catalog sources that are not byte-identical between the job's data root
    and the folder Airflow reads (relative paths). Catalog file lists never include error_manifest.*."""
    a, b = Path(cfg.data_root).resolve(), Path(cfg.airflow_data).resolve()
    if a == b:
        return []
    out = []
    for s in sources:
        for rel in s.get("files") or []:
            x, y = a / rel, b / rel
            if not (y.is_file() and filecmp.cmp(x, y, shallow=False)):
                out.append(rel)
    return sorted(set(out))


def preflight_problems(cfg: GenConfig, sources: list[dict]) -> list[str]:
    """Reasons the Airflow run could not use the same data / warehouse as this dashboard."""
    errors = []
    if Path(cfg.warehouse).resolve() != Path(cfg.airflow_warehouse).resolve():
        errors.append(f"The dashboard's warehouse ({cfg.warehouse}) is not the file Airflow writes "
                      f"({cfg.airflow_warehouse}); unset WAREHOUSE_DB and restart the dashboard.")
    diff = data_differences(cfg, sources)
    if diff:
        errors.append(f"{len(diff)} selected source file(s) differ between the data root ({cfg.data_root}) and "
                      f"the folder Airflow reads ({cfg.airflow_data}), so the Airflow run would not read what "
                      "the agent inspected: " + ", ".join(diff[:8]) + (" ..." if len(diff) > 8 else ""))
    return errors


def compose_brief(req: dict) -> str:
    """The --brief text: the user's prompt verbatim, then the id and sources it must use."""
    pid = req["pipeline_id"]
    lines = [req["prompt"], "",
             f"Pipeline id: {pid}  (write exactly dags/{pid}/pipeline.py, dags/{pid}_dag.py and "
             f"manifests/{pid}.yaml; do not create or modify any other pipeline's files).",
             "Source(s), relative to DATA_ROOT:"]
    for s in req["source_info"]:
        if s["kind"] == "table":
            lines.append(f"- {s['key']}  (SQLite table `{s['name']}` in {s['file']}, {s['columns']} columns)")
        else:
            rng = f", {s['date_range'][0]} .. {s['date_range'][1]}" if s.get("date_range") else ""
            lines.append(f"- {s['key']}  ({s['count']} file{'s' if s['count'] != 1 else ''}{rng})")
    lines += ["Orchestration: the dashboard runs this pipeline through Airflow right after you finish "
              "(it unpauses the DAG and triggers the first run), so the DAG must use schedule=\"@daily\", "
              "catchup=False and a fixed start_date in the past.",
              "The destination table must have a loaded_at column holding the UTC load timestamp "
              "(ISO 8601 with offset, e.g. 2026-11-28T02:30:00+00:00).",
              "Never open or list files named error_manifest.* (evaluation ground truth)."]
    return "\n".join(lines)


# ------------------------------------------------------------------ snapshot / restore
def _sha(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def _is_pyc(rel: str) -> bool:
    return "__pycache__" in rel.split("/")


def _walk(root: Path) -> tuple[dict, set]:
    files, dirs = {}, set()
    for top in WATCHED:
        base = root / top
        if not base.exists():
            continue
        dirs.add(top)
        for p in base.rglob("*"):
            rel = p.relative_to(root).as_posix()
            if p.is_dir() and not p.is_symlink():
                dirs.add(rel)
            elif p.is_file() or p.is_symlink():
                files[rel] = "pyc" if _is_pyc(rel) or p.is_symlink() else _sha(p)
    return files, dirs


def snapshot(root: Path, job_dir: Path) -> dict:
    files, dirs = _walk(root)
    for rel, h in files.items():
        if h != "pyc":
            dst = job_dir / "before" / rel
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(root / rel, dst)
    snap = {"files": files, "dirs": sorted(dirs)}
    (job_dir / "before.json").write_text(json.dumps(snap, indent=1, sort_keys=True))
    return snap


def changes(root: Path, snap: dict) -> dict:
    now, _ = _walk(root)
    before = snap["files"]
    return {"new": sorted(set(now) - set(before)),
            "modified": sorted(r for r in set(now) & set(before)
                               if before[r] != "pyc" and now[r] != before[r]),
            "deleted": sorted(r for r in set(before) - set(now) if before[r] != "pyc")}


def restore(root: Path, job_dir: Path) -> dict:
    """Put dags/ and manifests/ back to the snapshot; move what the job wrote into quarantine/."""
    snap = json.loads((job_dir / "before.json").read_text())
    ch = changes(root, snap)
    q = job_dir / "quarantine"
    for rel in ch["new"]:
        dst = q / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(root / rel), dst)
    for rel in ch["modified"]:
        dst = q / (rel + ".modified")
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(root / rel), dst)
        shutil.copy2(job_dir / "before" / rel, root / rel)
    for rel in ch["deleted"]:
        (root / rel).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(job_dir / "before" / rel, root / rel)
    keep = set(snap["dirs"])
    for top in WATCHED:
        base = root / top
        if base.exists():
            for d in sorted((p for p in base.rglob("*") if p.is_dir()), key=lambda p: -len(p.parts)):
                if d.relative_to(root).as_posix() not in keep and not any(d.iterdir()):
                    d.rmdir()
    return ch


# ------------------------------------------------------------------ warehouse helpers
def _tables(db: Path) -> set:
    if not Path(db).exists():
        return set()
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        return {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    finally:
        con.close()


def _count(db: Path, table: str) -> int | None:
    if table not in _tables(db):
        return None
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        return int(con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0])
    finally:
        con.close()


def _columns(db: Path, table: str) -> set:
    if table not in _tables(db):
        return set()
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        return {r[1] for r in con.execute(f'PRAGMA table_info("{table}")')}
    finally:
        con.close()


def _drop(db: Path, table: str) -> None:
    if TABLE_RE.match(table or "") and table in _tables(db):
        con = sqlite3.connect(db)
        try:
            con.execute(f'DROP TABLE IF EXISTS "{table}"')
            con.commit()
        finally:
            con.close()


def _claimed_tables(manifests: Path, except_pid: str) -> set:
    out = set()
    for p in manifests.glob("*.y*ml"):
        m = read_manifest(p)
        if m and m["pipeline_id"] != except_pid and isinstance(m.get("destination"), dict):
            out.add(str(m["destination"].get("table", "")))
    return out - {""}


def _pid_alive_and_ours(pid: int | None) -> bool:
    if not pid:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    try:
        cmd = subprocess.run(["ps", "-o", "command=", "-p", str(pid)], capture_output=True, text=True,
                             timeout=5).stdout
    except Exception:
        return False
    return "--brief" in cmd or str(RUNNER) in cmd


# ------------------------------------------------------------------ history
def append_history(path: Path, job: dict) -> None:
    rec = {"pipeline_id": job["pipeline_id"], "sources": job["sources"], "prompt": job["prompt"],
           "actor": job["actor"], "channel": "dashboard", "started_at": job.get("started_at"),
           "finished_at": job.get("finished_at"), "outcome": job["status"],
           "rows_loaded": job.get("rows_loaded"), "job_id": job["job_id"],
           "airflow_run_id": job.get("airflow_run_id"), "airflow_state": job.get("airflow_state")}
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a") as f:
        f.write(json.dumps(rec, sort_keys=True) + "\n")


# ------------------------------------------------------------------ manager
class Refused(Exception):
    pass


class JobManager:
    def __init__(self, cfg: GenConfig | None = None, airflow=None):
        self.cfg = cfg or GenConfig()
        self.airflow = airflow if airflow is not None else afl.AirflowCLI(Path(self.cfg.repo_root))
        self._lock = threading.RLock()
        self._active: str | None = None
        self._cancel: set = set()
        self._procs: dict = {}
        self._threads: dict = {}
        Path(self.cfg.jobs_dir).mkdir(parents=True, exist_ok=True)
        self.recover()

    # ---------------------------------------------------------- state on disk
    def job_dir(self, job_id: str) -> Path:
        if not JOB_ID_RE.match(job_id or ""):
            raise KeyError(job_id)
        return Path(self.cfg.jobs_dir) / job_id

    def get(self, job_id: str) -> dict | None:
        try:
            return json.loads((self.job_dir(job_id) / "job.json").read_text())
        except (KeyError, OSError, ValueError):
            return None

    def _save(self, job: dict) -> None:
        d = self.job_dir(job["job_id"])
        tmp = d / "job.json.tmp"
        tmp.write_text(json.dumps(job, indent=1, sort_keys=True))
        os.replace(tmp, d / "job.json")

    def _update(self, job_id: str, **kw) -> dict:
        with self._lock:
            job = self.get(job_id)
            job.update(kw)
            self._save(job)
            return job

    def list(self, limit: int = 50) -> list[dict]:
        out = []
        for d in sorted(Path(self.cfg.jobs_dir).glob("g-*"), reverse=True)[:limit]:
            j = self.get(d.name)
            if j:
                out.append(j)
        return out

    def active(self) -> dict | None:
        with self._lock:
            return self.get(self._active) if self._active else None

    def log_path(self, job_id: str) -> Path:
        return self.job_dir(job_id) / "log.txt"

    def read_log(self, job_id: str, offset: int = 0, limit: int = 256_000,
                 whole_lines: bool = True) -> tuple[str, int]:
        """Log text from byte `offset`; with whole_lines (while a job runs) it ends on a newline so
        a line or UTF-8 sequence is never split. Returns (text, next offset)."""
        try:
            with open(self.log_path(job_id), "rb") as f:
                f.seek(max(0, offset))
                chunk = f.read(limit)
        except (KeyError, OSError):
            return "", offset
        if whole_lines and chunk:
            cut = chunk.rfind(b"\n")
            if cut >= 0:
                chunk = chunk[:cut + 1]
            elif len(chunk) < limit:
                chunk = b""
        return chunk.decode("utf-8", errors="replace"), max(0, offset) + len(chunk)

    def _log(self, job_id: str, text: str) -> None:
        with open(self.log_path(job_id), "a") as f:
            f.write(f"[dashboard] {text}\n")

    # ---------------------------------------------------------- submit / cancel
    def submit(self, form: dict) -> str:
        """Validate and start a job; returns its id. Raises Refused(list_of_messages)."""
        with self._lock:
            if self._active:
                raise Refused([f"A generation job is already running ({self._active}). "
                               "Wait for it to finish or cancel it, then try again."])
            ok, msg = self.airflow.is_available()
            if not ok:
                raise Refused([msg])
            req, errors = validate_request(self.cfg, form)
            if errors:
                raise Refused(errors)
            job_id = self._new_id()
            d = Path(self.cfg.jobs_dir) / job_id
            d.mkdir(parents=True)
            brief = compose_brief(req)
            job = {"job_id": job_id, "kind": "generate", "pipeline_id": req["pipeline_id"], "sources": req["sources"],
                   "prompt": req["prompt"], "brief": brief, "covered_by": req["covered"],
                   "actor": actor(), "channel": "dashboard", "created_at": now_iso(),
                   "started_at": None, "finished_at": None, "status": "queued", "exit_code": None,
                   "error": "", "rows_loaded": None, "destination_table": None, "created_table": None,
                   "pid": None, "warnings": [], "time_limit_s": self.cfg.time_limit_s,
                   "data_root": str(self.cfg.data_root), "warehouse": str(self.cfg.warehouse),
                   "command": self.cfg.job_argv("<brief: see 'brief'>"), **self._airflow_fields(job_id)}
            return self._start(job, self._run)

    def submit_orchestrate(self, pid: str) -> str:
        """'Enable schedule and run now' for an existing pipeline: a tracked job that runs only the
        Airflow sequence. Raises Refused(list_of_messages)."""
        with self._lock:
            if self._active or active_job_on_disk(self.cfg.jobs_dir):
                raise Refused(["A generation job is running; try again after it finishes."])
            if pid in afl.PROTECTED:
                raise Refused([f"'{pid}' is one of the original DAGs; the dashboard does not change it."])
            man = read_manifest(self.cfg.manifests / f"{pid}.yaml") if ID_RE.match(pid or "") else None
            if not man or not (self.cfg.dags / f"{pid}_dag.py").is_file():
                raise Refused([f"No pipeline '{pid}' with manifests/{pid}.yaml and dags/{pid}_dag.py."])
            ok, msg = self.airflow.is_available()
            if not ok:
                raise Refused([msg])
            used = [s for s in build_catalog(self.cfg.data_root, self.cfg.manifests) if pid in s["covered_by"]]
            errors = preflight_problems(self.cfg, used)
            if errors:
                raise Refused(errors)
            job_id = self._new_id()
            (Path(self.cfg.jobs_dir) / job_id).mkdir(parents=True)
            table = str((man.get("destination") or {}).get("table", "")) if isinstance(man.get("destination"), dict) else ""
            job = {"job_id": job_id, "kind": "orchestrate", "pipeline_id": pid, "sources": [s["key"] for s in used],
                   "prompt": "", "brief": "", "covered_by": [], "actor": actor(), "channel": "dashboard",
                   "created_at": now_iso(), "started_at": None, "finished_at": None, "status": "queued",
                   "exit_code": None, "error": "", "rows_loaded": None, "destination_table": table,
                   "created_table": None, "pid": None, "warnings": [], "time_limit_s": self.cfg.time_limit_s,
                   "data_root": str(self.cfg.data_root), "warehouse": str(self.cfg.warehouse),
                   **self._airflow_fields(job_id)}
            return self._start(job, self._run_orchestrate)

    def _new_id(self) -> str:
        return f"g-{dt.datetime.now():%Y%m%d-%H%M%S}-{secrets.token_hex(2)}"

    def _airflow_fields(self, job_id: str) -> dict:
        return {"airflow_run_id": f"dashboard__{job_id}", "airflow_state": None, "dag_enabled": False,
                "airflow_schedule": None, "airflow_catchup": None, "airflow_failed_task": None,
                "airflow_log_tail": "",
                "airflow_data": f"{self.cfg.airflow_data_container} (host: {self.cfg.airflow_data})"}

    def _start(self, job: dict, target) -> str:
        job_id = job["job_id"]
        self._save(job)
        (self.job_dir(job_id) / "log.txt").write_text("")
        self._active = job_id
        t = threading.Thread(target=target, args=(job_id,), name=f"gen-{job_id}", daemon=True)
        self._threads[job_id] = t
        t.start()
        return job_id

    def cancel(self, job_id: str) -> str:
        """Ask the job thread to stop; it kills the whole process group within ~0.2 s."""
        with self._lock:
            job = self.get(job_id)
            if not job:
                raise KeyError(job_id)
            if job["status"] not in ACTIVE or job_id != self._active:
                return f"Job is {job['status']}; nothing to cancel."
            self._cancel.add(job_id)
        return "Cancelling: the process group is being stopped."

    def wait(self, job_id: str, timeout: float = 60) -> dict:
        t = self._threads.get(job_id)
        if t:
            t.join(timeout)
        return self.get(job_id)

    # ---------------------------------------------------------- process control
    def _kill(self, proc: subprocess.Popen) -> None:
        try:
            os.killpg(proc.pid, signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            return
        try:
            proc.wait(self.cfg.kill_grace_s)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
            proc.wait()

    def _pump(self, stream, log: Path) -> None:
        with open(log, "a", encoding="utf-8") as f:
            for raw in iter(stream.readline, b""):
                f.write(_withhold(raw.decode("utf-8", errors="replace")))
                f.flush()
        stream.close()

    def _spawn(self, job_id: str, argv: list, deadline: float) -> tuple[str, int | None]:
        """Run argv in its own process group; ('exit'|'cancelled'|'timeout', returncode)."""
        if job_id in self._cancel:
            return "cancelled", None
        env = {**os.environ, "PYTHONUNBUFFERED": "1"}
        proc = subprocess.Popen(argv, cwd=str(self.cfg.repo_root), stdin=subprocess.DEVNULL,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env,
                                start_new_session=True)
        with self._lock:
            self._procs[job_id] = proc
        self._update(job_id, pid=proc.pid)
        reader = threading.Thread(target=self._pump, args=(proc.stdout, self.log_path(job_id)), daemon=True)
        reader.start()
        outcome = "exit"
        while proc.poll() is None:
            if job_id in self._cancel:
                outcome = "cancelled"
            elif time.monotonic() > deadline:
                outcome = "timeout"
            if outcome != "exit":
                self._kill(proc)
                break
            time.sleep(0.2)
        proc.wait()
        reader.join(5)
        with self._lock:
            self._procs.pop(job_id, None)
        self._update(job_id, pid=None)
        if outcome == "exit" and job_id in self._cancel:
            outcome = "cancelled"
        return outcome, proc.returncode

    # ---------------------------------------------------------- the job
    def _run(self, job_id: str) -> None:
        job = self.get(job_id)
        jd = self.job_dir(job_id)
        pid = job["pipeline_id"]
        root = Path(self.cfg.repo_root)
        status, error, warnings = "failed", "", []
        snap = None
        try:
            snap = snapshot(root, jd)
            deadline = time.monotonic() + self.cfg.time_limit_s
            job = self._update(job_id, status="generating", started_at=now_iso())
            self._log(job_id, f"generating pipeline '{pid}' (time limit {self.cfg.time_limit_s / 60:g} min)")
            self._log(job_id, "command: " + " ".join(job["command"]))
            if answer_key_present(self.cfg.data_root):          # re-checked right before the agent starts
                error = ANSWER_KEY_REFUSAL
                return
            outcome, rc = self._spawn(job_id, self.cfg.job_argv(job["brief"]), deadline)
            self._update(job_id, exit_code=rc)
            if outcome != "exit":
                status, error = self._stopped(outcome)
                return
            error = self._check_generation(job_id, pid, rc, snap)
            if error:
                return
            man = yaml.safe_load((self.cfg.manifests / f"{pid}.yaml").read_text())
            table = str((man.get("destination") or {}).get("table", ""))
            self._update(job_id, status="orchestrating", destination_table=table, created_table=table)
            status, error, warnings = self._airflow_run(job_id, pid, table, deadline)
            if status != "succeeded":
                return
        except Exception as ex:                                    # never leave a job "running"
            error = f"internal error: {type(ex).__name__}: {ex}"
        finally:
            self._finish(job_id, status, error, warnings, snap is not None)

    def _stopped(self, outcome: str) -> tuple[str, str]:
        if outcome == "cancelled":
            return "cancelled", "Cancelled by the user; the process group was stopped."
        return "failed", (f"Time limit reached ({self.cfg.time_limit_s / 60:g} min); "
                          "the process group was stopped.")

    def _check_generation(self, job_id: str, pid: str, rc: int, snap: dict) -> str:
        text, _ = self.read_log(job_id, 0, 50_000_000, whole_lines=False)
        failed = re.findall(r"FINAL STATUS:\s*FAILED[^\n]*", text)
        if failed:
            return "The generation agent reported: " + failed[-1].strip()
        if rc != 0:
            tail = [ln for ln in text.splitlines() if ln.strip() and not ln.startswith("[dashboard]")][-3:]
            return f"The generation agent exited with code {rc}." + (f" Last output: {' | '.join(tail)}" if tail else "")
        if not re.search(r"FINAL STATUS:\s*SUCCESS", text):
            return "The generation agent finished without reporting 'FINAL STATUS: SUCCESS'."
        need = [f"dags/{pid}/pipeline.py", f"dags/{pid}_dag.py", f"manifests/{pid}.yaml"]
        missing = [n for n in need if not (Path(self.cfg.repo_root) / n).is_file()]
        if missing:
            return "The agent reported success but these files are missing: " + ", ".join(missing)
        ch = changes(Path(self.cfg.repo_root), snap)
        if ch["modified"] or ch["deleted"]:
            return "The agent changed existing files, which is not allowed: " + ", ".join(ch["modified"] + ch["deleted"])
        ok = lambda r: (r.startswith(f"dags/{pid}/") or r in need or _is_pyc(r))  # noqa: E731
        stray = [r for r in ch["new"] if not ok(r)]
        if stray:
            return "The agent wrote files outside its pipeline id: " + ", ".join(stray)
        try:
            man = yaml.safe_load((self.cfg.manifests / f"{pid}.yaml").read_text())
        except yaml.YAMLError as ex:
            return f"manifests/{pid}.yaml is not valid YAML: {ex}"
        if not isinstance(man, dict):
            return f"manifests/{pid}.yaml is not a mapping."
        if not isinstance(man.get("destination"), dict) or not isinstance(man.get("steps") or [], list):
            return f"manifests/{pid}.yaml needs a destination mapping and a steps list."
        if man.get("pipeline_id") != pid:
            return f"manifests/{pid}.yaml has pipeline_id {man.get('pipeline_id')!r}, expected {pid!r}."
        table = str((man.get("destination") or {}).get("table", ""))
        if not TABLE_RE.match(table):
            return f"manifests/{pid}.yaml has no valid destination.table ({table!r})."
        if table in _claimed_tables(self.cfg.manifests, pid):
            return f"Destination table '{table}' belongs to another pipeline; refusing to overwrite it."
        if table in _tables(self.cfg.warehouse):
            return f"Destination table '{table}' already exists in the warehouse; refusing to overwrite it."
        self._log(job_id, "generation finished: files and manifest look complete")
        return ""

    def _airflow_run(self, job_id: str, pid: str, table: str, deadline: float) -> tuple[str, str, list]:
        """The Airflow sequence for job_id; (status, error, warnings)."""
        job = self.get(job_id)
        used = [s for s in build_catalog(self.cfg.data_root, self.cfg.manifests) if s["key"] in set(job["sources"])]
        problems = preflight_problems(self.cfg, used)          # re-checked: data may change during generation
        if problems:
            return "failed", " ".join(problems), []
        self._log(job_id, f"orchestrating through Airflow: the run reads {job['airflow_data']} and writes "
                          f"table '{table}' in {self.cfg.airflow_warehouse}")
        res = afl.orchestrate(self.airflow, pid, job["airflow_run_id"], deadline,
                              log=lambda t: self._log(job_id, t), cancelled=lambda: job_id in self._cancel,
                              poll_s=self.cfg.airflow_poll_s,
                              on_unpaused=lambda: self._update(job_id, dag_enabled=True))
        self._update(job_id, airflow_state=res["state"], airflow_schedule=res["schedule"],
                     airflow_catchup=res["catchup"], airflow_failed_task=res["failed_task"],
                     airflow_log_tail=res["log_tail"], dag_enabled=res["ok"])
        if res["log_tail"]:
            self._log(job_id, "airflow log tail" + (f" (task {res['failed_task']})" if res["failed_task"] else "")
                      + ":\n" + res["log_tail"])
        if not res["ok"]:
            if res["outcome"] == "cancelled":
                return "cancelled", "Cancelled by the user; the DAG was paused again.", []
            if res["outcome"] == "timeout":
                return "failed", (f"Time limit reached ({self.cfg.time_limit_s / 60:g} min) while the Airflow run "
                                  "was not finished; the DAG was paused again."), []
            return "failed", res["error"], []
        rows = _count(self.cfg.warehouse, table) if table else None
        if table and rows is None:
            return "failed", (f"The Airflow run succeeded but table '{table}' is not in {self.cfg.warehouse}."), []
        warnings = []
        if table and "loaded_at" not in _columns(self.cfg.warehouse, table):
            warnings.append(f"Table '{table}' has no loaded_at column, so detection cannot check load freshness.")
        self._update(job_id, rows_loaded=rows)
        self._log(job_id, f"Airflow run {job['airflow_run_id']} succeeded: {rows} rows in '{table}'. "
                          f"DAG '{pid}' is enabled and runs on its schedule ({res['schedule']}).")
        return "succeeded", "", warnings

    def _run_orchestrate(self, job_id: str) -> None:
        job = self.get(job_id)
        status, error, warnings = "failed", "", []
        try:
            deadline = time.monotonic() + self.cfg.time_limit_s
            self._update(job_id, status="orchestrating", started_at=now_iso())
            status, error, warnings = self._airflow_run(job_id, job["pipeline_id"], job["destination_table"], deadline)
        except Exception as ex:
            error = f"internal error: {type(ex).__name__}: {ex}"
        finally:
            self._finish(job_id, status, error, warnings, False)

    def _finish(self, job_id: str, status: str, error: str, warnings: list, snapped: bool) -> None:
        try:
            if status != "succeeded":
                self._undo(job_id, snapped)
        except Exception as ex:
            error = (error + " | " if error else "") + f"cleanup error: {type(ex).__name__}: {ex}"
        if error:
            self._log(job_id, f"{status.upper()}: {error}")
        job = self._update(job_id, status=status, error=error, warnings=warnings, finished_at=now_iso(), pid=None)
        try:
            if job.get("kind", "generate") == "generate":
                append_history(self.cfg.history, job)
        finally:
            with self._lock:
                self._cancel.discard(job_id)
                if self._active == job_id:
                    self._active = None

    def _undo(self, job_id: str, snapped: bool) -> None:
        job = self.get(job_id)
        jd = self.job_dir(job_id)
        if job.get("dag_enabled"):                       # e.g. the server stopped mid-run
            try:
                self.airflow.pause(job["pipeline_id"])
                self._log(job_id, f"airflow: DAG '{job['pipeline_id']}' paused again")
            except Exception as ex:
                self._log(job_id, f"airflow: could not pause DAG '{job['pipeline_id']}': {ex}")
            self._update(job_id, dag_enabled=False)
        if job.get("created_table"):
            _drop(self.cfg.warehouse, job["created_table"])
            self._log(job_id, f"dropped partially loaded table '{job['created_table']}'")
        if snapped and (jd / "before.json").exists():
            ch = restore(Path(self.cfg.repo_root), jd)
            moved = ch["new"] + [r + " (restored)" for r in ch["modified"] + ch["deleted"]]
            self._update(job_id, quarantined=moved)
            if moved:
                self._log(job_id, "dags/ and manifests/ restored; moved into this job's quarantine/: "
                                  + ", ".join(moved))
            else:
                self._log(job_id, "dags/ and manifests/ unchanged")

    # ---------------------------------------------------------- restart
    def recover(self) -> None:
        """Jobs left active by a previous server process become 'interrupted' and are undone."""
        for d in sorted(Path(self.cfg.jobs_dir).glob("g-*")):
            job = self.get(d.name)
            if not job or job["status"] not in ACTIVE:
                continue
            if _pid_alive_and_ours(job.get("pid")):
                try:
                    os.killpg(job["pid"], signal.SIGTERM)
                    time.sleep(1)
                    os.killpg(job["pid"], signal.SIGKILL)
                except (ProcessLookupError, PermissionError):
                    pass
            self._log(job["job_id"], "the dashboard server restarted while this job was running")
            self._finish(job["job_id"], "interrupted",
                         "The dashboard server stopped while this job was running.", [],
                         job.get("kind", "generate") == "generate")
