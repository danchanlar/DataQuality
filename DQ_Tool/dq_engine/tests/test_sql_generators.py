import sys, os
# Add project root to PATH for imports
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT)

from dq_engine.sql_generators import generate_sql

def test(rule):
    print("\n=== Testing:", rule["rule_type"], "===")
    sql, params = generate_sql(rule)
    print("SQL:\n", sql)
    print("Params:", params)
    print("OK ✔")

def run_tests():
    # NOT NULL
    test({
        "rule_id": 1,
        "rule_type": "NOT_NULL",
        "target_schema": "dbo",
        "target_table": "Customers",
        "target_column": "Email",
        "parameters": {}
    })

    # VALUE RANGE
    test({
        "rule_id": 2,
        "rule_type": "VALUE_RANGE",
        "target_schema": "dbo",
        "target_table": "Orders",
        "target_column": "Amount",
        "parameters": {"min": 0, "max": 1000}
    })

    # STRING LENGTH
    test({
        "rule_id": 3,
        "rule_type": "STRING_LENGTH",
        "target_schema": "dbo",
        "target_table": "Customers",
        "target_column": "Phone",
        "parameters": {"min_len": 7, "max_len": 15}
    })

    # PATTERN MATCH
    test({
        "rule_id": 4,
        "rule_type": "PATTERN_MATCH",
        "target_schema": "dbo",
        "target_table": "Customers",
        "target_column": "Email",
        "parameters": {"like": "%@%"}
    })

    # UNIQUENESS
    test({
        "rule_id": 5,
        "rule_type": "UNIQUENESS",
        "target_schema": "dbo",
        "target_table": "Customers",
        "target_column": None,
        "parameters": {"key_columns": ["CustomerID"]}
    })

if __name__ == "__main__":
    run_tests()