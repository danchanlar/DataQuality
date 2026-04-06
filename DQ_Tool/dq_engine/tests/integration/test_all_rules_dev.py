# dq_engine/tests/integration/test_all_rules_dev.py

import pytest
from dq_engine.dq_engine import execute_session

pytestmark = pytest.mark.integration


def test_all_19_rules_execute_successfully(pool):
    """
    Runs all 19 rule types end-to-end against a real DEV database.

    REQUIREMENTS:
      - The listed tables/columns MUST exist in the target DB
      - Connection string must point to a DEV environment with real data
    """

    rules = [
        # 1 UNIQUENESS
        {
            "rule_id": None, "rule_type": "UNIQUENESS",
            "target_schema": "dbo", "target_table": "Customers",
            "parameters": {"key_columns": ["CustomerID"]},
        },

        # 2 NOT_NULL
        {
            "rule_id": None, "rule_type": "NOT_NULL",
            "target_schema": "dbo", "target_table": "Customers",
            "target_column": "Email", "parameters": {},
        },

        # 3 VALUE_RANGE
        {
            "rule_id": None, "rule_type": "VALUE_RANGE",
            "target_schema": "dbo", "target_table": "Orders",
            "target_column": "Amount", "parameters": {"min": 0, "max": 1_000_000},
        },

        # 4 PATTERN_MATCH
        {
            "rule_id": None, "rule_type": "PATTERN_MATCH",
            "target_schema": "dbo", "target_table": "Customers",
            "target_column": "Email", "parameters": {"like": "%@%"},
        },

        # 5 DUPLICATE_ROWS
        {
            "rule_id": None, "rule_type": "DUPLICATE_ROWS",
            "target_schema": "dbo", "target_table": "Customers",
            "parameters": {"columns": ["CustomerID", "Email"]},
        },

        # 6 STRING_LENGTH
        {
            "rule_id": None, "rule_type": "STRING_LENGTH",
            "target_schema": "dbo", "target_table": "Customers",
            "target_column": "Phone",
            "parameters": {"min_len": 3, "max_len": 20},
        },

        # 7 ALLOWED_VALUES
        {
            "rule_id": None, "rule_type": "ALLOWED_VALUES",
            "target_schema": "dbo", "target_table": "Customers",
            "target_column": "Status",
            "parameters": {"values": ["ACTIVE", "INACTIVE"]},
        },

        # 8 DATE_RANGE
        {
            "rule_id": None, "rule_type": "DATE_RANGE",
            "target_schema": "dbo", "target_table": "Orders",
            "target_column": "OrderDate",
            "parameters": {"min": "2000-01-01", "max": "2030-12-31"},
        },

        # 9 CROSS_COLUMN
        {
            "rule_id": None, "rule_type": "CROSS_COLUMN",
            "target_schema": "dbo", "target_table": "Orders",
            "parameters": {"left": "OrderDate", "right": "ShipDate", "op": "<="},
        },

        # 10 CONDITIONAL_REQUIRED
        {
            "rule_id": None, "rule_type": "CONDITIONAL_REQUIRED",
            "target_schema": "dbo", "target_table": "Orders",
            "parameters": {"cond": "[Status]='SHIPPED'", "required_col": "ShipDate"},
        },

        # 11 REFERENTIAL_INTEGRITY
        {
            "rule_id": None, "rule_type": "REFERENTIAL_INTEGRITY",
            "target_schema": "dbo", "target_table": "Orders",
            "target_column": "CustomerID",
            "parameters": {
                "parent_schema": "dbo",
                "parent_table": "Customers",
                "parent_col": "CustomerID",
            },
        },

        # 12 ORPHANED_ROWS
        {
            "rule_id": None, "rule_type": "ORPHANED_ROWS",
            "target_schema": "dbo", "target_table": "Orders",
            "target_column": "CustomerID",
            "parameters": {
                "parent_schema": "dbo",
                "parent_table": "Customers",
                "parent_col": "CustomerID",
            },
        },

        # 13 COMPLETENESS
        {
            "rule_id": None, "rule_type": "COMPLETENESS",
            "target_schema": "dbo", "target_table": "Customers",
            "parameters": {"required_columns": ["CustomerID", "Email"]},
        },

        # 14 NEGATIVE_VALUES
        {
            "rule_id": None, "rule_type": "NEGATIVE_VALUES",
            "target_schema": "dbo", "target_table": "Orders",
            "target_column": "Amount", "parameters": {},
        },

        # 15 NON_NEGATIVE_VALUES
        {
            "rule_id": None, "rule_type": "NON_NEGATIVE_VALUES",
            "target_schema": "dbo", "target_table": "Orders",
            "target_column": "Amount", "parameters": {},
        },

        # 16 POSITIVE_VALUES
        {
            "rule_id": None, "rule_type": "POSITIVE_VALUES",
            "target_schema": "dbo", "target_table": "Orders",
            "target_column": "Amount", "parameters": {},
        },

        # 17 CUSTOM_SQL_FILTER
        {
            "rule_id": None, "rule_type": "CUSTOM_SQL_FILTER",
            "target_schema": "dbo", "target_table": "Customers",
            "parameters": {"where": "[CustomerID] > ?", "params": [0]},
        },

        # 18 REGEX_MATCH
        {
            "rule_id": None, "rule_type": "REGEX_MATCH",
            "target_schema": "dbo", "target_table": "Customers",
            "target_column": "Email",
            "parameters": {"patindex": "%@%", "negate": False},
        },

        # 19 ROW_COUNT_MIN
        {
            "rule_id": None, "rule_type": "ROW_COUNT_MIN",
            "target_schema": "dbo", "target_table": "Customers",
            "parameters": {"min_rows": 1},
        },
    ]

    # Run the session
    session_result = execute_session(
        conn_pool=pool,
        rules=rules,
        executed_by="pytest-all-rules",
        max_workers=8,
        logger=None
    )

    assert session_result.total_rules == 19
    assert len(session_result.rule_results) == 19

    for result in session_result.rule_results:
        assert result.status in ("Succeeded", "Failed")
        assert result.started_at is not None
        assert result.completed_at is not None
        assert result.duration_ms >= 0
        assert result.row_count_checked is not None
        assert result.sql_preview is not None

    print("\n=== ALL 19 RULE TYPES EXECUTED SUCCESSFULLY ===")
    for res in session_result.rule_results:
        print(
            f"Rule {res.rule_id}: status={res.status}, "
            f"violations={res.violations_count}"
        )