"""
Phase 1 verification runner.

Verifies:
1) 50+ active rules can run in one session and cover 10+ schemas.
2) Execution path is SQL-only (no pandas.read_sql usage during session execution).
3) Rule execution rows are persisted to DQ.RuleExecutions.
4) Parallel run is faster than sequential run.
5) Session-level pass/fail counts match RuleExecutions aggregates (what UI displays).

Example (Windows auth):
python dq_engine/tests/integration/phase1_verification.py \
  --server "SANDBOX-SQL\\MSSQL2022" --database "CSBDATA_DEV" --pool-size 10 --parallel-workers 8
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from typing import Any, Dict, List, Tuple

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
sys.path.insert(0, ROOT)

from dq_engine.db_connection import SqlConnectionPool
from dq_engine.dq_engine import execute_session


def _load_candidate_rules(pool: SqlConnectionPool, min_rules: int) -> Tuple[List[Dict[str, Any]], int]:
    with pool.acquire() as conn:
        cur = conn.cursor()
        rows = cur.execute(
            """
            SELECT rule_id, rule_type, target_schema, target_table, target_column, parameters_json
            FROM DQ.Rules
            WHERE is_active = 1
            ORDER BY rule_id
            """
        ).fetchall()

    rules: List[Dict[str, Any]] = []
    schemas = set()
    for r in rows:
        params = json.loads(r.parameters_json) if getattr(r, "parameters_json", None) else {}
        rules.append(
            {
                "rule_id": int(r.rule_id),
                "rule_type": r.rule_type,
                "target_schema": r.target_schema,
                "target_table": r.target_table,
                "target_column": r.target_column,
                "parameters": params,
            }
        )
        schemas.add(str(r.target_schema))

    if len(rules) < min_rules:
        raise RuntimeError(f"Need at least {min_rules} active rules. Found {len(rules)}.")

    return rules[:min_rules], len(schemas)


def _rule_exec_stats(pool: SqlConnectionPool, session_id: int) -> Dict[str, int]:
    with pool.acquire() as conn:
        cur = conn.cursor()
        row = cur.execute(
            """
            SELECT
                COUNT(*) AS total_exec_rows,
                SUM(CASE WHEN status = 'Succeeded' THEN 1 ELSE 0 END) AS succeeded_rows,
                SUM(CASE WHEN status <> 'Succeeded' OR error_message IS NOT NULL THEN 1 ELSE 0 END) AS failed_rows
            FROM DQ.RuleExecutions
            WHERE session_id = ?
            """,
            (session_id,),
        ).fetchone()

    return {
        "total_exec_rows": int(row.total_exec_rows or 0),
        "succeeded_rows": int(row.succeeded_rows or 0),
        "failed_rows": int(row.failed_rows or 0),
    }


def _session_counts(pool: SqlConnectionPool, session_id: int) -> Dict[str, int]:
    with pool.acquire() as conn:
        cur = conn.cursor()
        row = cur.execute(
            """
            SELECT total_rules, passed, failed
            FROM DQ.ExecutionSessions
            WHERE session_id = ?
            """,
            (session_id,),
        ).fetchone()

    return {
        "total_rules": int(row.total_rules or 0),
        "passed": int(row.passed or 0),
        "failed": int(row.failed or 0),
    }


def _run_sql_only_guarded_session(
    pool: SqlConnectionPool,
    rules: List[Dict[str, Any]],
    executed_by: str,
    max_workers: int,
) -> Tuple[int, float]:
    # Guard against accidental pandas path usage in execution flow.
    import pandas as pd  # local import so script fails clearly if pandas unavailable

    original_read_sql = pd.read_sql

    def _blocked_read_sql(*_args, **_kwargs):
        raise RuntimeError("pandas.read_sql was called during execute_session; expected SQL-only execution path")

    pd.read_sql = _blocked_read_sql
    started = time.perf_counter()
    try:
        res = execute_session(pool, rules, executed_by=executed_by, max_workers=max_workers)
    finally:
        pd.read_sql = original_read_sql
    elapsed = time.perf_counter() - started
    return int(res.session_id), elapsed


def main() -> int:
    p = argparse.ArgumentParser(description="Phase 1 verification runner")
    p.add_argument("--server", required=True)
    p.add_argument("--database", required=True)
    p.add_argument("--auth-type", default="Windows Authentication")
    p.add_argument("--username", default="")
    p.add_argument("--password", default="")
    p.add_argument("--pool-size", type=int, default=10)
    p.add_argument("--rule-count", type=int, default=50)
    p.add_argument("--parallel-workers", type=int, default=8)
    p.add_argument("--executed-by", default="Phase1Verification")
    args = p.parse_args()

    pool = SqlConnectionPool(
        server=args.server,
        database=args.database,
        auth_type=args.auth_type,
        username=args.username,
        password=args.password,
        pool_size=args.pool_size,
    )

    rules, schema_count = _load_candidate_rules(pool, args.rule_count)
    print(f"Loaded {len(rules)} active rules from {schema_count} schema(s).")
    if schema_count < 10:
        print("[WARN] Less than 10 schemas are represented by active rules.")

    seq_session_id, seq_secs = _run_sql_only_guarded_session(
        pool,
        rules,
        executed_by=f"{args.executed_by}-SEQ",
        max_workers=1,
    )
    par_session_id, par_secs = _run_sql_only_guarded_session(
        pool,
        rules,
        executed_by=f"{args.executed_by}-PAR",
        max_workers=args.parallel_workers,
    )

    seq_exec = _rule_exec_stats(pool, seq_session_id)
    par_exec = _rule_exec_stats(pool, par_session_id)
    seq_sess = _session_counts(pool, seq_session_id)
    par_sess = _session_counts(pool, par_session_id)

    print("\n=== Phase 1 Verification Report ===")
    print(f"Sequential session_id={seq_session_id}, elapsed_sec={seq_secs:.2f}")
    print(f"Parallel   session_id={par_session_id}, elapsed_sec={par_secs:.2f}")
    print(f"Schema coverage: {schema_count} (target: >= 10)")
    print(f"Rule count used: {len(rules)} (target: >= 50)")
    print(f"RuleExecutions rows (SEQ): {seq_exec}")
    print(f"RuleExecutions rows (PAR): {par_exec}")
    print(f"ExecutionSessions counts (SEQ): {seq_sess}")
    print(f"ExecutionSessions counts (PAR): {par_sess}")

    checks = {
        "rules>=50": len(rules) >= 50,
        "schemas>=10": schema_count >= 10,
        "ruleexec_rows_seq": seq_exec["total_exec_rows"] == len(rules),
        "ruleexec_rows_par": par_exec["total_exec_rows"] == len(rules),
        "ui_counts_seq_match": seq_sess["passed"] == seq_exec["succeeded_rows"] and seq_sess["failed"] == seq_exec["failed_rows"],
        "ui_counts_par_match": par_sess["passed"] == par_exec["succeeded_rows"] and par_sess["failed"] == par_exec["failed_rows"],
        "parallel_faster": par_secs < seq_secs,
    }

    print("\nChecks:")
    for name, ok in checks.items():
        print(f"- {name}: {'PASS' if ok else 'FAIL'}")

    all_pass = all(checks.values())
    print("\nOVERALL:", "PASS" if all_pass else "FAIL")
    return 0 if all_pass else 1


if __name__ == "__main__":
    raise SystemExit(main())
