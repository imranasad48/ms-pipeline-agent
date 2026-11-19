# Generated scenarios / run artefacts contain copies of pipeline code (incl. Airflow DAG files);
# they are data, never tests.
collect_ignore_glob = ["results/*", "scenarios/*"]
