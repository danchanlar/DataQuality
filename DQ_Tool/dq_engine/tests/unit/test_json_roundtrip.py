
# dq_engine/tests/unit/test_json_roundtrip.py
import json
import os
import pytest
from dq_engine.dq_persistence import export_rules_to_json, import_rules_from_json


def test_export_rules_to_json_writes_expected(mock_conn, tmp_rules_rows, tmp_path):
    """
    Configure the mock connection with rows/columns so that any cursor created
    inside export_rules_to_json sees the same dataset.
    """
    # Configure the connection-level dataset for SELECT ... FROM DQ.Rules
    mock_conn._configured_columns = [
        "rule_id",
        "rule_type",
        "target_schema",
        "target_table",
        "target_column",
        "parameters_json",
        "is_active",
        "created_by",
        "created_at",
    ]
    # IMPORTANT: use the fixture that has all attributes (including is_active)
    # Our MockCursor returns tuples from _rows_buffer; but in previous conftest
    # we set fetchall() to return objects. For compatibility, just store objects:
    mock_conn._configured_rows = tmp_rules_rows[:]  # objects with attributes

    out_path = tmp_path / "rules.json"
    count = export_rules_to_json(mock_conn, str(out_path))

    # Now count should match the number of configured rows
    assert count == len(tmp_rules_rows)

    # Validate written JSON content
    data = json.loads(out_path.read_text(encoding="utf-8"))
    assert isinstance(data, list) and len(data) == len(tmp_rules_rows)
    assert data[0]["rule_id"] == 1
    assert data[0]["rule_type"] == "NOT_NULL"
    assert data[0]["parameters"] == {}
    assert data[0]["is_active"] is True
    assert data[1]["rule_id"] == 2
    assert data[1]["parameters"] == {"min": 0, "max": 100}

def test_import_rules_from_json_inserts_and_optionally_overwrites(mock_conn, tmp_path):
    items = [
        {
            "rule_type": "NOT_NULL",
            "target_schema": "dbo",
            "target_table": "Customers",
            "target_column": "Email",
            "parameters": {},
            "is_active": True,
        },
        {
            "rule_type": "VALUE_RANGE",
            "target_schema": "dbo",
            "target_table": "Orders",
            "target_column": "Amount",
            "parameters": {"min": 0, "max": 1000},
            "is_active": True,
        },
    ]
    path = tmp_path / "in.json"
    path.write_text(json.dumps(items), encoding="utf-8")

    # Run import without overwrite
    inserted = import_rules_from_json(mock_conn, str(path), overwrite=False)
    assert inserted == 2

    # Gather executed statements
    all_calls = []
    for c in mock_conn._cursors:
        all_calls.extend(c.calls)

    updates = [c for c in all_calls if c[0] == "execute" and "UPDATE DQ.Rules" in c[1]]
    inserts = [c for c in all_calls if c[0] == "execute" and "INSERT INTO DQ.Rules" in c[1]]

    assert len(updates) == 0
    assert len(inserts) == 2

    # Overwrite path
    mock_conn._cursors.clear()
    inserted2 = import_rules_from_json(mock_conn, str(path), overwrite=True)
    assert inserted2 == 2

    all_calls2 = []
    for c in mock_conn._cursors:
        all_calls2.extend(c.calls)
    updates2 = [c for c in all_calls2 if c[0] == "execute" and "UPDATE DQ.Rules" in c[1]]
    inserts2 = [c for c in all_calls2 if c[0] == "execute" and "INSERT INTO DQ.Rules" in c[1]]

    assert len(updates2) == 2  # one per item
    assert len(inserts2) == 2  # still inserts new rows