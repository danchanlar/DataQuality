
# dq_engine/tests/unit/conftest.py
import json
import datetime as dt
import itertools
import types
import pytest

# -------------------------
# Helpers
# -------------------------

def norm_sql(s: str) -> str:
    """Collapse whitespace for easy comparison."""
    if s is None:
        return ""
    return " ".join(s.split()).strip()

# -------------------------
# Mock DB primitives
# -------------------------

class MockCursor:
    """
    A stateful cursor that can:
      - return scalar from fetchone() for stored procs
      - return tabular rows for rule SQL (with .description and .fetchmany())
      - record executemany writes for violations
    """
    _id_counter = itertools.count(1000)

    def __init__(self, conn):
        self.conn = conn
        self.description = None
        self._mode = None
        self._rows_buffer = []
        self._fetchone_value = None
        self.calls = []  # records (sql, params) for execute/ executemany
        self.fast_executemany = False


    def execute(self, sql, params=None):
        self.calls.append(("execute", sql, list(params) if params else []))
        s = (sql or "").strip()

        if s.startswith("EXEC DQ.StartRuleExecution"):
            self._mode = "scalar"
            self._fetchone_value = next(self._id_counter)
            self.description = None

        elif s.startswith("EXEC DQ.CompleteRuleExecution"):
            self._mode = "none"
            self._fetchone_value = None
            self.description = None

        elif s.startswith("INSERT INTO DQ.Rules") and "OUTPUT INSERTED.rule_id" in s:
            self._mode = "scalar"
            self._fetchone_value = 42
            self.description = None

        # ✅ UPDATED: SELECT from DQ.Rules for export should prefer connection-configured rows
        elif s.upper().startswith("SELECT") and "FROM DQ.RULES" in s.upper():
            self._mode = "table"
            # If tests pre-configured connection-level rows, use them
            if self.conn._configured_rows:
                self._rows_buffer = list(self.conn._configured_rows)
                # If description not set, synthesize from configured_columns (objects are ok)
                self.description = [(c,) for c in (self.conn._configured_columns or [])]
            else:
                # fallback stub (rarely used)
                import types, json as _json
                self._rows_buffer = [types.SimpleNamespace(
                    rule_id=1,
                    rule_type="NOT_NULL",
                    target_schema="dbo",
                    target_table="T",
                    target_column="C",
                    parameters_json=_json.dumps({}),
                    is_active=1,
                    created_by="stub",
                    created_at=None,
                )]
                self.description = [
                    ("rule_id",), ("rule_type",), ("target_schema",), ("target_table",),
                    ("target_column",), ("parameters_json",), ("is_active",), ("created_by",), ("created_at",),
                ]

        else:
            # Treat as the actual rule violation SQL (execute_rule path)
            self._mode = "table"
            cols = self.conn._configured_columns or []
            self.description = [(c,) for c in cols] if cols else None
            self._rows_buffer = list(self.conn._configured_rows or [])

        return self

    def fetchone(self):
        if self._mode == "scalar":
            return (self._fetchone_value,)
        elif self._mode == "row_rule":
            return self._rows_buffer.pop(0) if self._rows_buffer else None
        return None

    def fetchall(self):
        # used by export_rules_to_json
        out = self._rows_buffer
        self._rows_buffer = []
        return out

    def fetchmany(self, size):
        if not self._rows_buffer:
            return []
        batch = self._rows_buffer[:size]
        self._rows_buffer = self._rows_buffer[size:]
        return batch

    def executemany(self, sql, seq_of_params):
        # record inserts into DQ.RuleViolations
        self.calls.append(("executemany", sql, list(seq_of_params)))
        # for our unit tests, nothing else needed

    def close(self):  # pragma: no cover
        pass


class MockConnection:
    """
    Connection that shares configuration for:
      _configured_columns: List[str]
      _configured_rows:    List[tuple] (aligned with columns)
    Also captures commit/rollback.
    """
    def __init__(self):
        self._configured_columns = []
        self._configured_rows = []
        self._cursors = []
        self._commits = 0
        self._rollbacks = 0

    def cursor(self):
        c = MockCursor(self)
        self._cursors.append(c)
        return c

    def commit(self):
        self._commits += 1

    def rollback(self):
        self._rollbacks += 1

    # For dq_persistence._get_connection "with" wrapper
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc, tb):
        if exc_type:
            self.rollback()
        else:
            self.commit()
        return False

# -------------------------
# Fixtures
# -------------------------

@pytest.fixture
def mock_conn():
    return MockConnection()

# dq_engine/tests/unit/conftest.py

@pytest.fixture
def tmp_rules_rows():
    """
    Sample rows returned by export_rules_to_json.
    Must include ALL selected columns:
    rule_id, rule_type, target_schema, target_table, target_column,
    parameters_json, is_active, created_by, created_at
    """
    import types, json, datetime as dt

    Row = types.SimpleNamespace
    now = dt.datetime(2025, 1, 1, 12, 0, 0)

    row1 = Row(
        rule_id=1,
        rule_type="NOT_NULL",
        target_schema="dbo",
        target_table="Customers",
        target_column="Email",
        parameters_json=json.dumps({}),
        is_active=1,
        created_by="userA",
        created_at=now,
    )

    row2 = Row(
        rule_id=2,
        rule_type="VALUE_RANGE",
        target_schema="dbo",
        target_table="Orders",
        target_column="Amount",
        parameters_json=json.dumps({"min": 0, "max": 100}),
        is_active=1,
        created_by="userB",
        created_at=now,
    )

    return [row1, row2]