
import sys, os
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT)

from dq_engine.db_connection import SqlConnectionPool
from dq_engine.dq_engine import execute_rule

def run_test_execute_rule():

    print("Creating pool...")
    pool = SqlConnectionPool(
        server=r"SANDBOX-SQL\MSSQL2022",
        database="CSBDATA_DEV",
        auth_type="Windows Authentication"
    )

    # CHANGE THIS TO A REAL RULE ID
    RULE_ID = 44  

    rule = {
        "rule_id": RULE_ID,
        "rule_type": "NOT_NULL",
        "target_schema": "dbo",
        "target_table": "Customers",
        "target_column": "Email",
        "parameters": {}
    }

    print("Executing rule...")

    with pool.acquire() as conn:
        result = execute_rule(conn, rule, session_id=None, logger=None)

    print("\n=== EXECUTION RESULT ===")
    print("Rule ID:         ", result.rule_id)
    print("Execution ID:    ", result.execution_id)
    print("Status:          ", result.status)
    print("Duration (ms):   ", result.duration_ms)
    print("Rows checked:    ", result.row_count_checked)
    print("Violations:      ", result.violations_count)
    print("Sample violations:", result.sample_violations)

    print("\nDone.")

if __name__ == "__main__":
    run_test_execute_rule()