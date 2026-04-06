# dq_engine/tests/integration/conftest.py
import os
import pytest
try:
    import pyodbc
except ModuleNotFoundError:
    pyodbc = None
from dq_engine.db_connection import SqlConnectionPool

@pytest.fixture(scope="session")
def conn_string(pytestconfig):
    # 1) Prefer CLI option
    cs = pytestconfig.getoption("conn_string")
    if cs:
        return cs

    # 2) Fallback to env vars
    driver = os.getenv("ODBC_DRIVER", "ODBC Driver 18 for SQL Server")
    server = os.getenv("DB_SERVER") or os.getenv("DB_HOST")
    database = os.getenv("DB_DATABASE") or os.getenv("DB_NAME")
    username = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")

    missing = [
        name for name, value in {
            "DB_SERVER/DB_HOST": server,
            "DB_DATABASE/DB_NAME": database,
            "DB_USER": username,
            "DB_PASSWORD": password,
        }.items() if not value
    ]
    if missing:
        pytest.skip(f"Missing database environment variables: {missing}")

    return (
        f"Driver={{{driver}}};"
        f"Server={server};"
        f"Database={database};"
        f"Uid={username};"
        f"Pwd={password};"
        f"Encrypt=no;"
        f"TrustServerCertificate=yes;"
    )

@pytest.fixture(scope="session")
def pool(conn_string):
    if pyodbc is None:
        pytest.skip("pyodbc is not installed in this environment.")

    parts = {}
    for chunk in conn_string.split(";"):
        if "=" not in chunk:
            continue
        key, value = chunk.split("=", 1)
        parts[key.strip().lower()] = value.strip()

    server = parts.get("server")
    database = parts.get("database")
    driver = parts.get("driver", "ODBC Driver 18 for SQL Server").strip("{}")
    username = parts.get("uid", "")
    password = parts.get("pwd", "")
    encrypt = parts.get("encrypt", "no")
    trust_cert = parts.get("trustservercertificate", "yes")

    if not server or not database:
        pytest.skip("Invalid connection string: missing Server or Database.")

    auth_type = "SQL Authentication" if username else "Windows Authentication"

    try:
        return SqlConnectionPool(
            server=server,
            database=database,
            auth_type=auth_type,
            username=username,
            password=password,
            driver=driver,
            pool_size=6,
            encrypt=encrypt,
            trust_cert=trust_cert,
        )
    except Exception as exc:
        pytest.skip(f"Skipping integration tests: cannot connect to SQL Server ({exc}).")