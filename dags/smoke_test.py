from airflow.decorators import dag, task
from datetime import datetime

@dag(schedule=None, start_date=datetime(2026, 11, 1), catchup=False, tags=["smoke"])
def smoke_test():
    @task
    def check_mounts():
        import os, sqlite3, json
        counts = {}
        root = "/opt/testbed/data"
        for src in sorted(os.listdir(root)):
            p = os.path.join(root, src)
            if os.path.isdir(p):
                counts[src] = sum(len(fs) for _, _, fs in os.walk(p))
        con = sqlite3.connect("/opt/warehouse/warehouse.db")
        con.execute("CREATE TABLE IF NOT EXISTS smoke (ts TEXT, counts TEXT)")
        con.execute("INSERT INTO smoke VALUES (datetime('now'), ?)", (json.dumps(counts),))
        con.commit(); con.close()
        print("TESTBED FILE COUNTS:", counts)
        return counts
    check_mounts()

smoke_test()
