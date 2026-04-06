
# dq_engine/tests/unit/test_result_parsing.py
import json
import pytest
from dq_engine.dq_engine import execute_rule, ExecutionResult

def _build_rule(rule_id=123, rule_type="NOT_NULL"):
    return {
        "rule_id": rule_id,
        "rule_type": rule_type,
        "target_schema": "dbo",
        "target_table": "Customers",
        "target_column": "Email",
        "parameters": {},
    }

def test_execute_rule_parses_rows_and_batches(mock_conn):
    """
    Simulate 3 violating rows; sample_limit=2; batch_size=2
    Expect:
      - row_count_checked = 3
      - violations_count  = 3
      - sample_violations length = 2
      - Complete stored proc called
    """
    # Configure the data returned for the rule SQL
    mock_conn._configured_columns = ["CustomerID", "Email"]
    mock_conn._configured_rows = [
        (1, None),
        (2, None),
        (3, None),
    ]

    res: ExecutionResult = execute_rule(
        mock_conn,
        rule_config=_build_rule(),
        session_id=555,
        logger=None,
        sample_limit=2,
        batch_size=2,
    )

    # Row-level assertions
    assert res.status == "Succeeded"
    assert res.row_count_checked == 3
    assert res.violations_count == 3
    assert len(res.sample_violations) == 2
    assert all(isinstance(x, dict) for x in res.sample_violations)
    # Verify sample content matches first rows
    assert res.sample_violations[0]["CustomerID"] == 1
    assert res.sample_violations[1]["CustomerID"] == 2

    # Verify we wrote into DQ.RuleViolations via executemany at least once
    all_calls = []
    for c in mock_conn._cursors:
        all_calls.extend(c.calls)
    ex_many = [c for c in all_calls if c[0] == "executemany"]
    assert len(ex_many) >= 1
    # Total inserted rows equals violations_count
    total_inserted = sum(len(batch) for _, _, batch in ex_many)
    assert total_inserted == res.violations_count

    # violation_row_key should be auto-populated from CustomerID
    first_batch = ex_many[0][2]
    assert first_batch[0][1] is not None
    assert str(first_batch[0][1]).startswith("CustomerID=")

def test_execute_rule_handles_empty_result(mock_conn):
    mock_conn._configured_columns = ["Id"]
    mock_conn._configured_rows = []  # no violations

    res = execute_rule(
        mock_conn,
        rule_config=_build_rule(),
        session_id=999,
        logger=None,
        sample_limit=5,
        batch_size=10,
    )
    assert res.status == "Succeeded"
    assert res.row_count_checked == 0
    assert res.violations_count == 0
    assert res.sample_violations == []

def test_execute_rule_handles_exception_and_sets_failed(mock_conn, monkeypatch):
    # Force the main SQL execute to raise by making cursor.execute blow up for non-proc SQL
    original_cursor = mock_conn.cursor
    def bad_cursor():
        c = original_cursor()
        # override execute: raise for non-EXEC commands
        orig_execute = c.execute
        def _execute(sql, params=None):
            s = (sql or "").strip()
            if not s.startswith("EXEC "):
                raise RuntimeError("Boom during execution")
            return orig_execute(sql, params)
        c.execute = _execute
        return c
    mock_conn.cursor = bad_cursor

    res = execute_rule(
        mock_conn,
        rule_config=_build_rule(),
        session_id=777,
        logger=None,
        sample_limit=1,
        batch_size=1,
    )
    assert res.status == "Failed"
    assert "Boom during execution" in (res.error_message or "")