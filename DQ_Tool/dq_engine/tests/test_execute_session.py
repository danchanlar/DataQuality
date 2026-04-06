
from multiprocessing import pool
import sys, os
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT)

from dq_engine.db_connection import SqlConnectionPool
from dq_engine.dq_engine import execute_session
from dq_engine.dq_logging import setup_logger


def run_test_execute_session():

    print("Creating connection pool...")
    pool = SqlConnectionPool(
        server=r"SANDBOX-SQL\MSSQL2022",
        database="CSBDATA_DEV",
        auth_type="Windows Authentication"
    )

    print("Loading rules from DB...")

    # Load all active rules
    with pool.acquire() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT rule_id, rule_type, target_schema, target_table, target_column, parameters_json
            FROM DQ.Rules
            WHERE is_active = 1
            ORDER BY rule_id
        """)

        rules = []
        for r in cur.fetchall():
            rules.append({
                "rule_id": r.rule_id,
                "rule_type": r.rule_type,
                "target_schema": r.target_schema,
                "target_table": r.target_table,
                "target_column": r.target_column,
                "parameters": eval(r.parameters_json) if r.parameters_json else {}
            })

    print(f"Loaded {len(rules)} rules.")

    if len(rules) == 0:
        print("❌ No rules to test. Add rules to DQ.Rules first.")
        return

    print("Executing rules in PARALLEL...")
    logger = setup_logger("dq_parallel_test.log")

    
    result = execute_session(
        conn_pool=pool,
        rules=rules,
        executed_by="Danai",
        max_workers=5,
        logger=logger,
        sample_limit=5,
        batch_size=500
    )


    print("\n=== SESSION COMPLETE ===")
    print("Session ID:      ", result.session_id)
    print("Total rules:     ", result.total_rules)
    print("Passed:          ", result.passed)
    print("Failed:          ", result.failed)
    print("Duration (ms):   ", result.duration_ms)

    print("\n=== RULE RESULTS ===")
    for r in result.rule_results:
        print(f"[Rule {r.rule_id}] Status={r.status}, Violations={r.violations_count}, Duration={r.duration_ms}ms")

    print("\nDone.")


if __name__ == "__main__":
    run_test_execute_session()