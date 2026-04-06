"""
Connection Pool with support for:
- Windows Authentication
- SQL Server Authentication
using the same pattern as the original tool.

This version integrates:
    get_connection(...)
    but upgraded into a pooled architecture.
"""

import threading
try:
    import pyodbc
except ModuleNotFoundError:
    pyodbc = None
from queue import Queue, Empty
from contextlib import contextmanager


def _require_pyodbc():
    if pyodbc is None:
        raise ModuleNotFoundError(
            "pyodbc is required for database connectivity. "
            "Install it in this environment (e.g. pip install pyodbc)."
        )


class SqlConnectionPool:
    """
    A dynamic connection pool supporting both SQL Auth and Windows Auth.

    Use:
        pool = SqlConnectionPool(
            server="localhost",
            database="CSBDATA_DEV",
            auth_type="Windows Authentication",
            username="",
            password="",
            pool_size=5
        )

        with pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
    """

    def __init__(self, server, database,
                 auth_type="Windows Authentication",
                 username="", password="",
                 driver="ODBC Driver 18 for SQL Server",
                 pool_size=5,
                 encrypt="no",
                 trust_cert="yes"):

        self.server = server
        self.database = database
        self.auth_type = auth_type
        self.username = username
        self.password = password
        self.driver = driver
        self.pool_size = pool_size
        self.encrypt = encrypt
        self.trust_cert = trust_cert

        self._queue = Queue(maxsize=self.pool_size)
        self._lock = threading.Lock()

        self._init_pool()

    # -------------------------------------------------------------------
    # CONNECTION STRING BUILDER 
    # -------------------------------------------------------------------
    def _build_connection_string(self):
        if self.auth_type == "Windows Authentication":
            conn_str = (
                f"DRIVER={{{self.driver}}};"
                f"SERVER={self.server};"
                f"DATABASE={self.database};"
                f"Trusted_Connection=yes;"
            )
        else:  # SQL Authentication
            conn_str = (
                f"DRIVER={{{self.driver}}};"
                f"SERVER={self.server};"
                f"DATABASE={self.database};"
                f"UID={self.username};"
                f"PWD={self.password};"
            )

        # Optional — required for Azure or ODBC 18 stronger encryption
        conn_str += f"Encrypt={self.encrypt};"
        conn_str += f"TrustServerCertificate={self.trust_cert};"

        return conn_str

    # -------------------------------------------------------------------
    # POOL INITIALIZATION
    # -------------------------------------------------------------------
    def _connect(self):
        _require_pyodbc()
        conn_str = self._build_connection_string()
        return pyodbc.connect(conn_str, autocommit=False)

    def _init_pool(self):
        for _ in range(self.pool_size):
            self._queue.put(self._connect())

    # -------------------------------------------------------------------
    # HEALTH CHECK (if connection died, recreate)
    # -------------------------------------------------------------------
    def _ensure_alive(self, conn):
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            return conn
        except:
            try:
                conn.close()
            except:
                pass
            return self._connect()

    # -------------------------------------------------------------------
    # ACQUIRE / RELEASE (context manager)
    # -------------------------------------------------------------------
    @contextmanager
    def acquire(self, timeout=30):
        conn = None
        try:
            conn = self._queue.get(timeout=timeout)
            conn = self._ensure_alive(conn)
            yield conn
            conn.commit()
        except Exception:
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
            raise
        finally:
            if conn:
                self._queue.put(conn)

    # -------------------------------------------------------------------
    # CLOSE POOL
    # -------------------------------------------------------------------
    def close_all(self):
        while True:
            try:
                conn = self._queue.get_nowait()
            except Empty:
                break
            try:
                conn.close()
            except:
                pass


# -----------------------------------------------------------------------
# HELPER FUNCTIONS
# -----------------------------------------------------------------------

def get_connection(server, database, auth_type='Windows Authentication', username='', password=''):
    """Create a SINGLE connection (no pooling) — same as old code."""
    _require_pyodbc()
    if auth_type == 'Windows Authentication':
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};DATABASE={database};Trusted_Connection=yes;"
        )
    else:
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};DATABASE={database};UID={username};PWD={password};"
        )

    return pyodbc.connect(conn_str)


# Pandas helpers:
# Week‑1 SQL‑pushdown means these will be removed in Week 2
import pandas as pd

def get_tables(conn):
    query = """
        SELECT TABLE_SCHEMA + '.' + TABLE_NAME as TableName
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_SCHEMA, TABLE_NAME;
    """
    df = pd.read_sql(query, conn)
    return df['TableName'].tolist()


def get_columns(conn, table_name):
    schema, table = table_name.split('.') if '.' in table_name else ('dbo', table_name)
    query = f"""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
        ORDER BY ORDINAL_POSITION;
    """
    df = pd.read_sql(query, conn)
    return df['COLUMN_NAME'].tolist(), df


def load_table_data(conn, table_name, limit=None):
    query = f"SELECT * FROM {table_name}"
    if limit:
        query += f" ORDER BY (SELECT NULL) OFFSET 0 ROWS FETCH NEXT {limit} ROWS ONLY"
    return pd.read_sql(query, conn)