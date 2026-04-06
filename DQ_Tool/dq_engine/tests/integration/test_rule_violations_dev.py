import pytest
import json
from dq_engine.dq_engine import execute_rule

pytestmark = pytest.mark.integration  # file-level mark

def test_rule_violations_inserts(pool):
    """
    Execute a NOT_NULL rule, ensure DQ.RuleViolations receives rows
    and violation_details contains valid JSON.
    """
    rule = {
        "rule_id": None,
        "rule_type": "NOT_NULL",
        "target_schema": "dbo",
        "target_table": "Customers",
        "target_column": "Email",
        "parameters": {},
    }

    # Execute the rule
    with pool.acquire() as conn:
        result = execute_rule(
            conn=conn,
            rule_config=rule,
            session_id=None,
            logger=None,
            sample_limit=5,
            batch_size=500,
        )

    execution_id = result.execution_id
    assert execution_id is not None

    # Read violations
    with pool.acquire() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT violation_row_key, violation_details
            FROM DQ.RuleViolations
            WHERE execution_id = ?
        """, (execution_id,))
        rows = cur.fetchall()

    assert len(rows) == result.violations_count and len(rows) > 0, \
        f"Expected > 0 violations, got {len(rows)}"

    # Validate JSON
    for _, details_json in rows:
        try:
            d = json.loads(details_json)
            assert isinstance(d, dict)
        except Exception:
            pytest.fail(f"Invalid JSON in violation_details: {details_json}")

    print(f"Inserted {len(rows)} violation rows for execution_id={execution_id}")