
"""
Minimal end-to-end test for a single rule using the connection pool and generators.

IMPORTANT:
- Adjust TARGET_SCHEMA/TABLE/COLUMN to a real table in your database before running.
- This script demonstrates the flow: create session -> insert rule -> start exec -> run SQL -> log violations -> complete exec.
"""

import json
import datetime
from dq_engine.db_connection import SqlConnectionPool
from dq_engine.sql_generators import generate_sql

# ---- CONFIG: CHANGE THESE TO REAL TARGETS ----
TARGET_SCHEMA = "apata"
TARGET_TABLE = "ApiUsers"
TARGET_COLUMN = "Password"  # for NOT_NULL / PATTERN / RANGE etc.

def main():
    pool = SqlConnectionPool( server=r"SANDBOX-SQL\MSSQL2022", database="CSBDATA_DEV", auth_type="Windows Authentication" )

    # 1) Start session
    with pool.acquire() as conn:
        cur = conn.cursor()
        cur.execute("EXEC DQ.StartExecutionSession @executed_by=?, @total_rules=?", ("Danai", 1))
        session_id = cur.fetchone()[0]
        print(f"Started session_id={session_id}")

    # 2) Insert a test rule (NOT_NULL)
    rule_type = "NOT_NULL"
    parameters = {}  # no parameters needed
    with pool.acquire() as conn:
        cur = conn.cursor()
        insert_sql = """
            INSERT INTO DQ.Rules (rule_type, target_schema, target_table, target_column, parameters_json, is_active)
            OUTPUT INSERTED.rule_id
            VALUES (?, ?, ?, ?, ?, 1)
        """
        cur.execute(insert_sql, (rule_type, TARGET_SCHEMA, TARGET_TABLE, TARGET_COLUMN, json.dumps(parameters)))
        rule_id = cur.fetchone()[0]
        print(f"Inserted rule_id={rule_id}")

    # 3) Start rule execution
    with pool.acquire() as conn:
        cur = conn.cursor()
        cur.execute("EXEC DQ.StartRuleExecution @rule_id=?, @session_id=?", (rule_id, session_id))
        execution_id = cur.fetchone()[0]
        print(f"Started execution_id={execution_id}")

    # 4) Generate SQL and execute to fetch violations
    rule = {
        "rule_id": rule_id,
        "rule_type": rule_type,
        "target_schema": TARGET_SCHEMA,
        "target_table": TARGET_TABLE,
        "target_column": TARGET_COLUMN,
        "parameters": parameters
    }
    sql_text, params = generate_sql(rule)

    violations = []
    row_count_checked = 0
    with pool.acquire() as conn:
        cur = conn.cursor()
        cur.execute(sql_text, params)
        columns = [d[0] for d in cur.description] if cur.description else []
        for row in cur.fetchall():
            row_dict = {columns[i]: row[i] for i in range(len(columns))}
            row_count_checked += 1
            # Build a violation payload
            violation_row_key = None
            try:
                # try to compose a simple row key from PK-like columns if any
                for key_candidate in ("Id", "ID", "id"):
                    if key_candidate in row_dict:
                        violation_row_key = str(row_dict[key_candidate])
                        break
            except Exception:
                pass
            violations.append((
                violation_row_key,
                json.dumps(row_dict, default=str)
            ))

    violations_count = len(violations)
    status = "Succeeded"
    result_summary = f"{violations_count} violation(s) found."

    # 5) Insert violations (bulk via executemany for simplicity)
    if violations_count > 0:
        with pool.acquire() as conn:
            cur = conn.cursor()
            cur.fast_executemany = True
            cur.executemany(
                "INSERT INTO DQ.RuleViolations (execution_id, violation_row_key, violation_details) VALUES (?, ?, ?)",
                [(execution_id, v[0], v[1]) for v in violations]
            )
            print(f"Inserted {violations_count} violations")

    # 6) Complete rule execution
    with pool.acquire() as conn:
        cur = conn.cursor()
        cur.execute(
            "EXEC DQ.CompleteRuleExecution @execution_id=?, @status=?, @result_summary=?, @row_count_checked=?, @violations_count=?, @error_message=?",
            (execution_id, status, result_summary, row_count_checked, violations_count, None)
        )
        print("Completed execution.")

    print("Done.")

if __name__ == "__main__":
    main()