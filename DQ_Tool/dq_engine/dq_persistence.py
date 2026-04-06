

"""
JSON export/import of DQ.Rules for backwards compatibility.

- export_rules_to_json(conn_or_pool, file_path)
- import_rules_from_json(conn_or_pool, file_path, overwrite=False)

Schema mirrors DQ.Rules with parsed parameters.
"""
from __future__ import annotations
import json
import datetime as dt
from typing import Any, Dict, List, Union

from dq_engine.db_connection import SqlConnectionPool

def _get_connection(pool_or_conn):
    if isinstance(pool_or_conn, SqlConnectionPool):
        return pool_or_conn.acquire()
    # Wrap raw connection so you can use `with`
    class _Wrapper:
        def __init__(self, conn):
            self.conn = conn
        def __enter__(self):
            return self.conn
        def __exit__(self, exc_type, exc, tb):
            if exc:
                try: self.conn.rollback()
                except Exception: pass
            else:
                try: self.conn.commit()
                except Exception: pass
            return False
    return _Wrapper(pool_or_conn)

def export_rules_to_json(pool_or_conn: Union[SqlConnectionPool, Any], file_path: str) -> int:
    """Export DQ.Rules to a JSON file. Returns number of rules exported."""
    with _get_connection(pool_or_conn) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT rule_id, rule_type, target_schema, target_table, target_column,
                   parameters_json, is_active, created_by, created_at
              FROM DQ.Rules
              ORDER BY rule_id
            """
        )
        rows = cur.fetchall()

    payload: List[Dict[str, Any]] = []
    for r in rows:
        try:
            params = json.loads(r.parameters_json) if getattr(r, 'parameters_json', None) else {}
        except Exception:
            params = {}

        # AFTER (defensive getattr):
        payload.append({
            'rule_id': int(getattr(r, 'rule_id')),
            'rule_type': getattr(r, 'rule_type', None),
            'target_schema': getattr(r, 'target_schema', None),
            'target_table': getattr(r, 'target_table', None),
            'target_column': getattr(r, 'target_column', None),
            'parameters': params,
            'is_active': bool(getattr(r, 'is_active', 0)),
            'created_by': getattr(r, 'created_by', None),
            'created_at': (getattr(r, 'created_at', None).isoformat()
                   if getattr(r, 'created_at', None) else None),
    })

    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    return len(payload)

# ... keep the rest of the file unchanged ...

def import_rules_from_json(pool_or_conn: Union[SqlConnectionPool, Any], file_path: str, overwrite: bool = False) -> int:
    """
    Import rules from JSON file into DQ.Rules.
    If overwrite=True, existing rules with the same (rule_type, target_schema, target_table, target_column, parameters_json)
    are deactivated and replaced.

    Returns number of inserted rules.
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        items = json.load(f)
    if not isinstance(items, list):
        raise ValueError('JSON must be a list of rule objects')

    inserted = 0
    with _get_connection(pool_or_conn) as conn:
        cur = conn.cursor()

        # If overwrite: fetch all existing active rules into Python and match there.
        # This avoids any SQL-side ntext vs nvarchar(max) comparison issues.
        existing_by_signature: dict = {}
        if overwrite:
            cur.execute(
                """
                SELECT rule_id,
                       CAST(rule_type       AS nvarchar(max)) AS rule_type,
                       CAST(target_schema   AS nvarchar(max)) AS target_schema,
                       CAST(target_table    AS nvarchar(max)) AS target_table,
                       CAST(target_column   AS nvarchar(max)) AS target_column,
                       CAST(parameters_json AS nvarchar(max)) AS parameters_json
                  FROM DQ.Rules
                 WHERE is_active = 1
                """
            )
            for row in cur.fetchall():
                sig = (
                    (row[1] or '').strip(),
                    (row[2] or '').strip(),
                    (row[3] or '').strip(),
                    (row[4] or '').strip(),
                    (row[5] or '').strip(),
                )
                existing_by_signature.setdefault(sig, []).append(int(row[0]))

        for it in items:
            rule_type = it.get('rule_type')
            target_schema = it.get('target_schema')
            target_table = it.get('target_table')
            target_column = it.get('target_column')
            params_json = json.dumps(it.get('parameters') or {})
            is_active = 1 if it.get('is_active', True) else 0

            if overwrite:
                # Match in Python; deactivate by rule_id (integer — no ntext issues)
                sig = (
                    (rule_type or '').strip(),
                    (target_schema or '').strip(),
                    (target_table or '').strip(),
                    (target_column or '').strip(),
                    params_json.strip(),
                )
                for rid in existing_by_signature.get(sig, []):
                    cur.execute(
                        "UPDATE DQ.Rules SET is_active = 0 WHERE rule_id = ?",
                        (rid,)
                    )

            # Insert with explicit created_by to avoid NULL default issues
            cur.execute(
                """
                INSERT INTO DQ.Rules
                    (rule_type, target_schema, target_table, target_column, parameters_json, is_active, created_by)
                VALUES (?, ?, ?, ?, ?, ?, COALESCE(SUSER_SNAME(), SYSTEM_USER, 'UNKNOWN'))
                """,
                (rule_type, target_schema, target_table, target_column, params_json, is_active)
            )
            inserted += 1

    return inserted