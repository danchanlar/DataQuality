# dq_engine/tests/unit/test_sql_generators.py
import pytest
from dq_engine.sql_generators import generate_sql
from dq_engine.tests.unit.test_utils import norm_sql

# Each case: (rule_dict, expected_sql, expected_params)
CASES = [
    # UNIQUENESS
    (
        {
            "rule_id": 1,
            "rule_type": "UNIQUENESS",
            "target_schema": "dbo",
            "target_table": "T",
            "target_column": None,
            "parameters": {"key_columns": ["A", "B"]},
        },
        "SELECT [A], [B], COUNT(*) AS dup_count FROM [dbo].[T] GROUP BY [A], [B] HAVING COUNT(*) > 1",
        [],
    ),
    # NOT_NULL
    (
        {
            "rule_id": 2,
            "rule_type": "NOT_NULL",
            "target_schema": "dbo",
            "target_table": "Customers",
            "target_column": "Email",
            "parameters": {},
        },
        "SELECT * FROM [dbo].[Customers] WHERE [Email] IS NULL",
        [],
    ),
    # VALUE_RANGE (both bounds)
    (
        {
            "rule_id": 3,
            "rule_type": "VALUE_RANGE",
            "target_schema": "dbo",
            "target_table": "Orders",
            "target_column": "Amount",
            "parameters": {"min": 0, "max": 1000},
        },
        "SELECT * FROM [dbo].[Orders] WHERE [Amount] < ? OR [Amount] > ?",
        [0, 1000],
    ),
    # VALUE_RANGE (min only)
    (
        {
            "rule_id": 31,
            "rule_type": "VALUE_RANGE",
            "target_schema": "dbo",
            "target_table": "Orders",
            "target_column": "Amount",
            "parameters": {"min": 5},
        },
        "SELECT * FROM [dbo].[Orders] WHERE [Amount] < ?",
        [5],
    ),
    # VALUE_RANGE (max only)
    (
        {
            "rule_id": 32,
            "rule_type": "VALUE_RANGE",
            "target_schema": "dbo",
            "target_table": "Orders",
            "target_column": "Amount",
            "parameters": {"max": 10},
        },
        "SELECT * FROM [dbo].[Orders] WHERE [Amount] > ?",
        [10],
    ),
    # PATTERN_MATCH
    (
        {
            "rule_id": 4,
            "rule_type": "PATTERN_MATCH",
            "target_schema": "dbo",
            "target_table": "Customers",
            "target_column": "Email",
            "parameters": {"like": "%@%"},
        },
        "SELECT * FROM [dbo].[Customers] WHERE [Email] NOT LIKE ?",
        ["%@%"],
    ),
    # DUPLICATE_ROWS
    (
        {
            "rule_id": 5,
            "rule_type": "DUPLICATE_ROWS",
            "target_schema": "dbo",
            "target_table": "T",
            "target_column": None,
            "parameters": {"columns": ["C1", "C2", "C3"]},
        },
        "SELECT [C1], [C2], [C3], COUNT(*) AS dup_count FROM [dbo].[T] GROUP BY [C1], [C2], [C3] HAVING COUNT(*) > 1",
        [],
    ),
    # STRING_LENGTH (both bounds)
    (
        {
            "rule_id": 6,
            "rule_type": "STRING_LENGTH",
            "target_schema": "dbo",
            "target_table": "Customers",
            "target_column": "Phone",
            "parameters": {"min_len": 7, "max_len": 15},
        },
        "SELECT * FROM [dbo].[Customers] WHERE LEN([Phone]) < ? OR LEN([Phone]) > ?",
        [7, 15],
    ),
    # ALLOWED_VALUES
    (
        {
            "rule_id": 7,
            "rule_type": "ALLOWED_VALUES",
            "target_schema": "dbo",
            "target_table": "T",
            "target_column": "Status",
            "parameters": {"values": ["A", "B", "C"]},
        },
        "SELECT * FROM [dbo].[T] WHERE [Status] NOT IN (?, ?, ?) OR [Status] IS NULL",
        ["A", "B", "C"],
    ),
    # DATE_RANGE (both)
    (
        {
            "rule_id": 8,
            "rule_type": "DATE_RANGE",
            "target_schema": "dbo",
            "target_table": "Orders",
            "target_column": "OrderDate",
            "parameters": {"min": "2020-01-01", "max": "2025-12-31"},
        },
        "SELECT * FROM [dbo].[Orders] WHERE [OrderDate] < ? OR [OrderDate] > ?",
        ["2020-01-01", "2025-12-31"],
    ),
    # CROSS_COLUMN
    (
        {
            "rule_id": 9,
            "rule_type": "CROSS_COLUMN",
            "target_schema": "dbo",
            "target_table": "T",
            "target_column": None,
            "parameters": {"op": "<=", "left": "StartDate", "right": "EndDate"},
        },
        "SELECT * FROM [dbo].[T] WHERE NOT ([StartDate] <= [EndDate])",
        [],
    ),
    # CONDITIONAL_REQUIRED
    (
        {
            "rule_id": 10,
            "rule_type": "CONDITIONAL_REQUIRED",
            "target_schema": "dbo",
            "target_table": "T",
            "target_column": None,
            "parameters": {"cond": "[Status] = 'ACTIVE'", "required_col": "EndDate"},
        },
        "SELECT * FROM [dbo].[T] WHERE ([Status] = 'ACTIVE') AND [EndDate] IS NULL",
        [],
    ),
    # REFERENTIAL_INTEGRITY
    (
        {
            "rule_id": 11,
            "rule_type": "REFERENTIAL_INTEGRITY",
            "target_schema": "dbo",
            "target_table": "Child",
            "target_column": "ParentId",  # child key
            "parameters": {"parent_schema": "dbo", "parent_table": "Parent", "parent_col": "Id"},
        },
        "SELECT c.* FROM [dbo].[Child] AS c LEFT JOIN [dbo].[Parent] AS p ON c.[ParentId] = p.[Id] "
        "WHERE p.[Id] IS NULL AND c.[ParentId] IS NOT NULL",
        [],
    ),
    # ORPHANED_ROWS (alias)
    (
        {
            "rule_id": 12,
            "rule_type": "ORPHANED_ROWS",
            "target_schema": "dbo",
            "target_table": "Child",
            "target_column": "ParentId",
            "parameters": {"parent_schema": "dbo", "parent_table": "Parent", "parent_col": "Id"},
        },
        "SELECT c.* FROM [dbo].[Child] AS c LEFT JOIN [dbo].[Parent] AS p ON c.[ParentId] = p.[Id] "
        "WHERE p.[Id] IS NULL AND c.[ParentId] IS NOT NULL",
        [],
    ),
    # COMPLETENESS
    (
        {
            "rule_id": 13,
            "rule_type": "COMPLETENESS",
            "target_schema": "dbo",
            "target_table": "T",
            "target_column": None,
            "parameters": {"required_columns": ["A", "B", "C"]},
        },
        "SELECT * FROM [dbo].[T] WHERE [A] IS NULL OR [B] IS NULL OR [C] IS NULL",
        [],
    ),
    # NEGATIVE_VALUES
    (
        {
            "rule_id": 14,
            "rule_type": "NEGATIVE_VALUES",
            "target_schema": "dbo",
            "target_table": "T",
            "target_column": "Amount",
            "parameters": {},
        },
        "SELECT * FROM [dbo].[T] WHERE [Amount] < 0",
        [],
    ),
    # NON_NEGATIVE_VALUES (per your implementation, violations are < 0)
    (
        {
            "rule_id": 15,
            "rule_type": "NON_NEGATIVE_VALUES",
            "target_schema": "dbo",
            "target_table": "T",
            "target_column": "Amount",
            "parameters": {},
        },
        "SELECT * FROM [dbo].[T] WHERE [Amount] < 0",
        [],
    ),
    # POSITIVE_VALUES
    (
        {
            "rule_id": 16,
            "rule_type": "POSITIVE_VALUES",
            "target_schema": "dbo",
            "target_table": "T",
            "target_column": "Amount",
            "parameters": {},
        },
        "SELECT * FROM [dbo].[T] WHERE [Amount] <= 0",
        [],
    ),
    # CUSTOM_SQL_FILTER
    (
        {
            "rule_id": 17,
            "rule_type": "CUSTOM_SQL_FILTER",
            "target_schema": "dbo",
            "target_table": "T",
            "target_column": None,
            "parameters": {"where": "[A] = ? AND [B] > ?", "params": [1, 2]},
        },
        "SELECT * FROM [dbo].[T] WHERE [A] = ? AND [B] > ?",
        [1, 2],
    ),
    # REGEX_MATCH (approx via PATINDEX; in your code both branches use '= 0')
    (
        {
            "rule_id": 18,
            "rule_type": "REGEX_MATCH",
            "target_schema": "dbo",
            "target_table": "T",
            "target_column": "Col",
            "parameters": {"patindex": "%[0-9][0-9][0-9]%", "negate": False},
        },
        "SELECT * FROM [dbo].[T] WHERE PATINDEX(?, [Col]) = 0",
        ["%[0-9][0-9][0-9]%"],
    ),
    # ROW_COUNT_MIN
    (
        {
            "rule_id": 19,
            "rule_type": "ROW_COUNT_MIN",
            "target_schema": "dbo",
            "target_table": "T",
            "target_column": None,
            "parameters": {"min_rows": 5},
        },
        "WITH cte AS ( SELECT COUNT(*) AS cnt FROM [dbo].[T] ) SELECT * FROM cte WHERE cnt < ?",
        [5],
    ),
]

@pytest.mark.parametrize("rule, expected_sql, expected_params", CASES)
def test_generate_sql_matches(rule, expected_sql, expected_params):
    sql, params = generate_sql(rule)
    assert norm_sql(sql) == norm_sql(expected_sql), f"\nExpected:\n{expected_sql}\nGot:\n{sql}"
    assert params == expected_params