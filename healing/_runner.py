"""Standalone pipeline runner (executed as a subprocess, never imported).

Runs a pipeline package's steps in order against a given DATA_ROOT / warehouse
and writes per-check results as JSON:
  step:<fn>        pass | fail | skipped
  validate:<name>  pass | fail | skipped      (from the pipeline's own validate())
After the first failing step the remaining steps are 'skipped' (like Airflow).
"""
import argparse
import contextlib
import importlib
import io
import json
import os
import re
import sys
import traceback

sys.dont_write_bytecode = True


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pipeline-root", required=True)   # dir that contains dags/
    ap.add_argument("--pid", required=True)
    ap.add_argument("--steps", required=True)           # comma separated function names
    ap.add_argument("--data-root", required=True)
    ap.add_argument("--warehouse", required=True)
    ap.add_argument("--staging-dir", required=True)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()

    os.environ.update(DATA_ROOT=os.path.abspath(a.data_root),
                      WAREHOUSE_DB=os.path.abspath(a.warehouse),
                      STAGING_DIR=os.path.abspath(a.staging_dir))
    os.makedirs(a.staging_dir, exist_ok=True)
    sys.path.insert(0, os.path.join(os.path.abspath(a.pipeline_root), "dags"))
    steps = [s for s in a.steps.split(",") if s]
    results, errors = {}, {}
    try:
        mod = importlib.import_module(f"{a.pid}.pipeline")
    except Exception as e:  # import-time failure: nothing can run
        results.update({f"step:{s}": "fail" if i == 0 else "skipped" for i, s in enumerate(steps)})
        errors[f"step:{steps[0]}"] = f"import error: {type(e).__name__}: {e}"
        _write(a.out, results, errors)
        return
    failed = False
    for fn in steps:
        key = f"step:{fn}"
        if failed:
            results[key] = "skipped"
            continue
        buf = io.StringIO()
        try:
            with contextlib.redirect_stdout(buf):
                getattr(mod, fn)()
            results[key] = "pass"
        except Exception as e:
            results[key] = "fail"
            msg = f"{type(e).__name__}: {e}"
            tb = traceback.extract_tb(e.__traceback__)[-1]
            errors[key] = f"{msg}  (at {os.path.basename(tb.filename)}:{tb.lineno})"
            failed = fn != "validate"
        if fn == "validate":
            found = re.findall(r"\[validate\] (PASS|FAIL) (\S+)", buf.getvalue())
            for status, name in found:
                results[f"validate:{name}"] = status.lower()
            if results[key] == "fail" and not found:
                results["validate:(unparsed)"] = "fail"
    _write(a.out, results, errors)


def _write(path, results, errors):
    with open(path, "w") as f:
        json.dump({"results": results, "errors": errors}, f, indent=1, sort_keys=True)


if __name__ == "__main__":
    main()
