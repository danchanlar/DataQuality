import pytest
from dq_engine.dq_engine import execute_session

pytestmark = pytest.mark.integration  # file-level mark

def test_execute_session_writes_to_DQ_tables(pool):
    """
    End-to-end integration test.
    Runs 3 rules and verifies DQ.ExecutionSessions & DQ.RuleExecutions data.
    """
    rules = [
        {
            "rule_id": None,
            "rule_type": "NOT_NULL",
            "target_schema": "dbo",
            "target_table": "Customers",
            "target_column": "Email",
            "parameters": {},
        },
        {
            "rule_id": None,
            "rule_type": "VALUE_RANGE",
            "target_schema": "dbo",
            "target_table": "Orders",
            "target_column": "Amount",
            "parameters": {"min": 0, "max": 1_000_000},
        },
        {
            "rule_id": None,
            "rule_type": "PATTERN_MATCH",
            "target_schema": "dbo",
            "target_table": "Customers",
            "target_column": "Email",
            "parameters": {"like": "%@%"},
        },
    ]

    session_result = execute_session(
        conn_pool=pool,
        rules=rules,
        executed_by="pytest-integration",
        max_workers=3,
        logger=None,
        sample_limit=5,
        batch_size=500,
    )

    session_id = session_result.session_id
    assert session_id is not None
    assert session_result.total_rules == 3
    assert session_result.passed + session_result.failed == 3

    # Validate DQ.ExecutionSessions
    with pool.acquire() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT session_id, started_at, completed_at, total_rules, passed, failed
            FROM DQ.ExecutionSessions
            WHERE session_id = ?
        """, (session_id,))
        row = cur.fetchone()
    assert row is not None
    assert row.total_rules == 3
    assert row.passed + row.failed == 3
    assert row.started_at is not None
    assert row.completed_at is not None

    # Validate DQ.RuleExecutions
    with pool.acquire() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT execution_id, rule_id, session_id,
                   started_at, completed_at, status,
                   row_count_checked, violations_count,
                   result_summary
            FROM DQ.RuleExecutions
            WHERE session_id = ?
            ORDER BY execution_id
        """, (session_id,))
        rows = cur.fetchall()

    assert len(rows) == 3
    for r in rows:
        assert r.started_at is not None
        assert r.status in ("Succeeded", "Failed")
        assert r.row_count_checked is not None
        assert r.violations_count is not None
        assert isinstance(r.result_summary, str)
        assert r.session_id == session_id

    # Debug summary
    print("\n--- Session Summary (DEV) ---")
    print(f"Session ID: {session_id}")
    print(f"Passed: {session_result.passed}  Failed: {session_result.failed}")
    for rr in session_result.rule_results:
        print(f"  Rule {rr.rule_id}: status={rr.status}, violations={rr.violations_count}")