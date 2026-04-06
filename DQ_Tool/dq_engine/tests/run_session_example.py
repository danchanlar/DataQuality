
# example: tests/run_session_example.py
from dq_engine.db_connection import SqlConnectionPool
from dq_engine.dq_logging import setup_logger
from dq_engine.dq_engine import execute_session

# 1) Create a pool (choose your auth type)
pool = SqlConnectionPool(
    server="localhost",
    database="CSBDATA_DEV",
    auth_type="Windows Authentication",  # or "SQL Server Authentication"
    username="",                         # if SQL Auth
    password="",                         # if SQL Auth
    pool_size=5
)

# 2) Prepare rules (you can use rule_id of existing rules, or define new ones inline)
rules = [
    {
        "rule_id": None,
        "rule_type": "NOT_NULL",
        "target_schema": "dbo",
        "target_table": "Customers",
        "target_column": "Email",
        "parameters": {}
    },
    {
        "rule_id": None,
        "rule_type": "VALUE_RANGE",
        "target_schema": "dbo",
        "target_table": "Orders",
        "target_column": "Amount",
        "parameters": {"min": 0}
    },
    {
        "rule_id": None,
        "rule_type": "STRING_LENGTH",
        "target_schema": "dbo",
        "target_table": "Customers",
        "target_column": "Phone",
        "parameters": {"min_len": 7, "max_len": 15}
    }
]

# 3) Logger
logger = setup_logger("dq_engine.log")

# 4) Execute in parallel (max_workers defaults to pool size)
session_result = execute_session(
    pool,
    rules,
    executed_by="Danai",
    max_workers=5,
    logger=logger,
    sample_limit=5,  # keep summary light
    batch_size=1000  # insert violations in chunks
)

print("Session:", session_result.session_id, "passed:", session_result.passed, "failed:", session_result.failed)
for r in session_result.rule_results:
    print(r.rule_id, r.status, r.violations_count, r.duration_ms, "ms")