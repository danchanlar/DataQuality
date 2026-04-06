"""
Parallel Data Quality Rule Execution Test

This script runs multiple rules in parallel threads using the
SqlConnectionPool and the SQL generator module.

It simulates:
- inserting multiple test rules
- running each rule independently
- recording violations
- completing each execution
- running all of this in parallel with N threads

Before running:
    1. Update TARGET_SCHEMA / TARGET_TABLE / COLUMNS below.
    2. Ensure environment variables for MSSQL_* are set.
"""

import json
import threading
import datetime
from queue import Queue
from dq_engine.db_connection import SqlConnectionPool
from dq_engine.sql_generators import generate_sql

# ======== CONFIGURE THIS PART FOR YOUR DATABASE =========
TARGET_SCHEMA = "dbo"
TARGET_TABLE = "YourTable"
TEST_COLUMNS = ["YourColumn1", "YourColumn2", "YourColumn3"]
# =========================================================


def create_test_rules(pool, num_rules):
    """
    Inserts num_rules test NOT_NULL rules into DQ.Rules.
    Returns list of rule_ids.
    """
    rule_ids = []
    with pool.acquire() as conn:
        cur = conn.cursor()
        for i in range(num_rules):
            column = TEST_COLUMNS[i % len(TEST_COLUMNS)]
            rule_type = "NOT_NULL"
            params_json = json.dumps({})

            cur.execute(
                """
                INSERT INTO DQ.Rules (rule_type, target_schema, target_table, target_column, parameters_json, is_active)
                OUTPUT INSERTED.rule_id
                VALUES (?, ?, ?, ?, ?, 1)
                """,
                (rule_type, TARGET_SCHEMA, TARGET_TABLE, column, params_json)
            )
            rule_id = cur.fetchone()[0]
            rule_ids.append(rule_id)
            print(f"[INIT] Created rule_id={rule_id} for column={column}")
    return rule_ids


def run_rule(pool, rule_id, session_id):
    """
    Runs a single rule execution inside a thread.
    """
    try:
        # ---- 1) Load rule metadata ----
        with pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT rule_type, target_schema, target_table, target_column, parameters_json
                FROM DQ.Rules WHERE rule_id = ?
            """, (rule_id,))
            row = cur.fetchone()

        if not row:
            print(f"[ERROR] Rule {rule_id} not found!")
            return

        rule_type, schema, table, column, params_json = row
        parameters = json.loads(params_json) if params_json else {}

        rule = {
            "rule_id": rule_id,
            "rule_type": rule_type,
            "target_schema": schema,
            "target_table": table,
            "target_column": column,
            "parameters": parameters
        }

        # ---- 2) Begin execution ----
        with pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute("EXEC DQ.StartRuleExecution @rule_id=?, @session_id=?", (rule_id, session_id))
            execution_id = cur.fetchone()[0]
            print(f"[THREAD {threading.get_ident()}] Started exec {execution_id} for rule {rule_id}")

        # ---- 3) Generate SQL ----
        sql_text, params = generate_sql(rule)

        # ---- 4) Execute SQL & collect violations ----
        violations = []
        row_count_checked = 0

        with pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute(sql_text, params)
            cols = [d[0] for d in cur.description] if cur.description else []
            for r in cur.fetchall():
                d = {cols[i]: r[i] for i in range(len(cols))}
                row_count_checked += 1
                violations.append((
                    None,  # row_key (optional)
                    json.dumps(d, default=str)
                ))

        violations_count = len(violations)

        # ---- 5) Insert violations ----
        if violations:
            with pool.acquire() as conn:
                cur = conn.cursor()
                cur.fast_executemany = True
                cur.executemany(
                    """
                    INSERT INTO DQ.RuleViolations (execution_id, violation_row_key, violation_details)
                    VALUES (?, ?, ?)
                    """,
                    [(execution_id, v[0], v[1]) for v in violations]
                )

        # ---- 6) Complete execution ----
        result_summary = f"{violations_count} violation(s)."

        with pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                EXEC DQ.CompleteRuleExecution
                    @execution_id=?, @status=?, @result_summary=?, @row_count_checked=?, @violations_count=?, @error_message=?
                """,
                (execution_id, "Succeeded", result_summary, row_count_checked, violations_count, None)
            )

        print(f"[THREAD {threading.get_ident()}] Finished exec {execution_id} with {violations_count} violations.")

    except Exception as e:
        print(f"[ERROR THREAD {threading.get_ident()}] {str(e)}")


def main():
    pool = SqlConnectionPool()

    # ---- A. Create a session ----
    with pool.acquire() as conn:
        cur = conn.cursor()
        cur.execute("EXEC DQ.StartExecutionSession @executed_by=?, @total_rules=?", ("ParallelTest", 5))
        session_id = cur.fetchone()[0]
        print(f"Session started: {session_id}")

    # ---- B. Insert test rules ----
    rule_ids = create_test_rules(pool, num_rules=5)

    # ---- C. Run rules in parallel threads ----
    threads = []

    for rule_id in rule_ids:
        t = threading.Thread(target=run_rule, args=(pool, rule_id, session_id))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    print("Parallel execution completed.")


if __name__ == "__main__":
    main()