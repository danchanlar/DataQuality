
# dq_engine/dq_engine.py
"""
Core execution engine for Data Quality rules.

Features:
- execute_rule(conn, rule_config, session_id, logger, sample_limit, batch_size)
- execute_session(conn_pool, rules, max_workers)
- Writes lifecycle logs into DQ.RuleExecutions / DQ.RuleViolations / DQ.ExecutionSessions
- Parallel execution via ThreadPoolExecutor (one pooled connection per thread)

Rule config contract (dict):
{
  "rule_id": int | None (if rule not yet in DB),
  "rule_type": str,
  "target_schema": str,
  "target_table": str,
  "target_column": Optional[str],
  "parameters": dict
}
"""
from __future__ import annotations
import json
import time
import datetime as dt
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

from dq_engine.sql_generators import generate_sql
from dq_engine.db_connection import SqlConnectionPool


@dataclass
class ExecutionResult:
    rule_id: int
    execution_id: int
    status: str
    started_at: dt.datetime
    completed_at: dt.datetime
    duration_ms: int
    row_count_checked: int
    violations_count: int
    error_message: Optional[str] = None
    sample_violations: List[Dict[str, Any]] = field(default_factory=list)
    sql_preview: Optional[str] = None


@dataclass
class SessionResult:
    session_id: int
    total_rules: int
    passed: int
    failed: int
    started_at: dt.datetime
    completed_at: dt.datetime
    duration_ms: int
    rule_results: List[ExecutionResult] = field(default_factory=list)


def _start_session(conn, executed_by: Optional[str], total_rules: int) -> int:
    cur = conn.cursor()
    cur.execute("EXEC DQ.StartExecutionSession @executed_by=?, @total_rules=?", (executed_by, total_rules))
    sid = int(cur.fetchone()[0])
    return sid


def _set_session_parallelism(conn, session_id: int, parallelism_level: int):
    """Populate optional DQ.ExecutionSessions.parallelism_level if column exists."""
    cur = conn.cursor()
    cur.execute(
        """
        IF COL_LENGTH('DQ.ExecutionSessions', 'parallelism_level') IS NOT NULL
        BEGIN
            UPDATE DQ.ExecutionSessions
               SET parallelism_level = ?
             WHERE session_id = ?;
        END
        """,
        (parallelism_level, session_id),
    )


def _start_rule_execution(conn, rule_id: int, session_id: Optional[int]) -> int:
    cur = conn.cursor()
    cur.execute("EXEC DQ.StartRuleExecution @rule_id=?, @session_id=?", (rule_id, session_id))
    eid = int(cur.fetchone()[0])
    return eid


def _complete_rule_execution(
    conn,
    execution_id: int,
    status: str,
    result_summary: Optional[str],
    row_count_checked: Optional[int],
    violations_count: Optional[int],
    error_message: Optional[str],
):
    cur = conn.cursor()
    cur.execute(
        (
            "EXEC DQ.CompleteRuleExecution "
            "@execution_id=?, @status=?, @result_summary=?, @row_count_checked=?, @violations_count=?, @error_message=?"
        ),
        (execution_id, status, result_summary, row_count_checked, violations_count, error_message),
    )


def _update_session_summary(
    conn,
    session_id: int,
    total_rules: int,
    passed: int,
    failed: int,
    errors_count: Optional[int] = None,
    failed_rule_ids: Optional[List[int]] = None,
    worker_error_details: Optional[List[Dict[str, Any]]] = None,
):
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE DQ.ExecutionSessions
           SET completed_at = SYSUTCDATETIME(),
               total_rules = ?,
               passed = ?,
               failed = ?
         WHERE session_id = ?
        """,
        (total_rules, passed, failed, session_id),
    )

    if errors_count is None:
        errors_count = failed

    if failed_rule_ids is None:
        failed_rule_ids = []
    if worker_error_details is None:
        worker_error_details = []

    # Include any rule_id from worker-level failures.
    for we in worker_error_details:
        rid = we.get("rule_id")
        if rid is not None:
            failed_rule_ids.append(int(rid))

    # Fallback: if we know the session failed but no rule ids were collected in-memory,
    # derive them from persisted rule execution rows.
    if failed > 0 and not failed_rule_ids:
        cur.execute(
            """
            SELECT DISTINCT rule_id
              FROM DQ.RuleExecutions
             WHERE session_id = ?
               AND status = 'Failed'
               AND rule_id IS NOT NULL
            """,
            (session_id,),
        )
        failed_rule_ids = [int(row[0]) for row in cur.fetchall()]

    cur.execute(
        """
        IF COL_LENGTH('DQ.ExecutionSessions', 'errors') IS NOT NULL
        BEGIN
            UPDATE DQ.ExecutionSessions
               SET errors = ?
             WHERE session_id = ?;
        END
        """,
        (errors_count, session_id),
    )

    cur.execute(
        """
        IF COL_LENGTH('DQ.ExecutionSessions', 'failed_rule_ids') IS NOT NULL
        BEGIN
            UPDATE DQ.ExecutionSessions
               SET failed_rule_ids = ?
             WHERE session_id = ?;
        END
        """,
        (json.dumps(sorted(set(int(rid) for rid in failed_rule_ids))), session_id),
    )

    cur.execute(
        """
        IF COL_LENGTH('DQ.ExecutionSessions', 'worker_error_details_json') IS NOT NULL
        BEGIN
            UPDATE DQ.ExecutionSessions
               SET worker_error_details_json = ?
             WHERE session_id = ?;
        END
        """,
        (json.dumps(worker_error_details, default=str), session_id),
    )


def _fetch_rule_by_id(conn, rule_id: int) -> Dict[str, Any]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT rule_id, rule_type, target_schema, target_table, target_column, parameters_json
          FROM DQ.Rules
         WHERE rule_id = ?
        """,
        (rule_id,),
    )
    row = cur.fetchone()
    if not row:
        raise ValueError(f"Rule {rule_id} not found")
    params = json.loads(row.parameters_json) if getattr(row, 'parameters_json', None) else {}
    return {
        "rule_id": int(row.rule_id),
        "rule_type": row.rule_type,
        "target_schema": row.target_schema,
        "target_table": row.target_table,
        "target_column": row.target_column,
        "parameters": params,
    }

#helper for automated produce of violation_row_key 
def _derive_violation_row_key(row_data: Dict[str, Any], rule_config: Dict[str, Any]) -> Optional[str]:
    """
    Build a stable row key for violations using PK-like columns when available.
    Priority:
      1) <TargetTable>ID
      2) ID / RowID
      3) First non-null *ID column
      4) First non-null column in the row
    Returns None only if all values are null/empty.
    """
    if not row_data:
        return None

    table_name = str(rule_config.get("target_table") or "").strip().lower()

    # Case-insensitive lookup map while preserving original column names.
    normalized = {str(k).lower(): k for k in row_data.keys()}

    preferred = []
    if table_name:
        preferred.append(f"{table_name}id")
    preferred.extend(["id", "rowid"])

    for key in preferred:
        original = normalized.get(key)
        if original is None:
            continue
        value = row_data.get(original)
        if value is not None and value != "":
            return f"{original}={value}"

    for lowered, original in normalized.items():
        if lowered.endswith("id"):
            value = row_data.get(original)
            if value is not None and value != "":
                return f"{original}={value}"

    for original, value in row_data.items():
        if value is not None and value != "":
            return f"{original}={value}"

    return None


def execute_rule(
    conn,
    rule_config: Dict[str, Any],
    session_id: Optional[int] = None,
    logger: Optional[logging.Logger] = None,
    sample_limit: int = 10,
    batch_size: int = 1000,
) -> ExecutionResult:
    """
    Execute a single rule:
    - Generate SQL via sql_generators
    - Start RuleExecution
    - Stream violations into DQ.RuleViolations (batched)
    - Complete RuleExecution
    Returns ExecutionResult
    """
    rule_id = int(rule_config["rule_id"]) if rule_config.get("rule_id") is not None else None

    # Ensure rule exists in DB (rule_id required for logging)
    if rule_id is None:
        # Insert into DQ.Rules first
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO DQ.Rules (rule_type, target_schema, target_table, target_column, parameters_json, is_active)
            OUTPUT INSERTED.rule_id
            VALUES (?, ?, ?, ?, ?, 1)
            """,
            (
                rule_config["rule_type"],
                rule_config["target_schema"],
                rule_config["target_table"],
                rule_config.get("target_column"),
                json.dumps(rule_config.get("parameters") or {}),
            ),
        )
        rule_id = int(cur.fetchone()[0])

    started_at = dt.datetime.utcnow()
    execution_id = _start_rule_execution(conn, rule_id, session_id)

    status = "Succeeded"
    error_message = None
    row_count_checked = 0
    violations_count = 0
    sample_violations: List[Dict[str, Any]] = []

    # Generate SQL
    sql_text, params = generate_sql(rule_config)
    sql_preview = sql_text.strip()

    if logger:
        logger.info(f"[rule_id={rule_id}] Starting execution_id={execution_id}")
        logger.debug(f"[rule_id={rule_id}] SQL Generated:\n{sql_preview}\nParams={params}")

    try:
        cur = conn.cursor()
        cur.execute(sql_text, params)
        columns = [d[0] for d in cur.description] if cur.description else []

        batch: List[Tuple[Optional[str], str]] = []
        # Stream rows; don't load all into memory
        fetched = cur.fetchmany(batch_size)
        while fetched:
            for r in fetched:
                d = {columns[i]: r[i] for i in range(len(columns))}
                d_serializable = {
                    k: (str(v) if isinstance(v, (bytes, memoryview)) else v)
                    for k, v in d.items()
                }
                violation_row_key = _derive_violation_row_key(d_serializable, rule_config)
                row_count_checked += 1
                if len(sample_violations) < sample_limit:
                    sample_violations.append(d_serializable)
                batch.append((violation_row_key, json.dumps(d_serializable, default=str)))
                if len(batch) >= batch_size:
                    _insert_violation_batch(conn, execution_id, batch)
                    violations_count += len(batch)
                    batch.clear()
            fetched = cur.fetchmany(batch_size)
        # flush remaining
        if batch:
            _insert_violation_batch(conn, execution_id, batch)
            violations_count += len(batch)
            batch.clear()

    except Exception as ex:
        status = "Failed"
        error_message = str(ex)[:3900]
        if logger:
            logger.exception(f"[rule_id={rule_id}] Execution failed: {error_message}")

    completed_at = dt.datetime.utcnow()
    duration_ms = int((completed_at - started_at).total_seconds() * 1000)

    # Summary JSON into result_summary (NVARCHAR(4000))
    summary_obj = {
        "sql": sql_preview[:2500],
        "params": params,
        "row_count_checked": row_count_checked,
        "violations_count": violations_count,
        "sample_violations": sample_violations,
        "duration_ms": duration_ms,
    }
    result_summary = json.dumps(summary_obj, default=str)
    if len(result_summary) > 3800:
        result_summary = result_summary[:3800]

    _complete_rule_execution(
        conn,
        execution_id,
        status,
        result_summary,
        row_count_checked,
        violations_count,
        error_message,
    )

    if logger:
        logger.info(
            f"[rule_id={rule_id}] Completed execution_id={execution_id} status={status} "
            f"violations={violations_count} duration_ms={duration_ms}"
        )

    return ExecutionResult(
        rule_id=rule_id,
        execution_id=execution_id,
        status=status,
        started_at=started_at,
        completed_at=completed_at,
        duration_ms=duration_ms,
        row_count_checked=row_count_checked,
        violations_count=violations_count,
        error_message=error_message,
        sample_violations=sample_violations,
        sql_preview=sql_preview[:1000],
    )


def _insert_violation_batch(conn, execution_id: int, rows: List[Tuple[Optional[str], str]]):
    cur = conn.cursor()
    cur.fast_executemany = True
    cur.executemany(
        """
        INSERT INTO DQ.RuleViolations (execution_id, violation_row_key, violation_details)
        VALUES (?, ?, ?)
        """,
        [(execution_id, rk, details) for (rk, details) in rows],
    )


def execute_session(
    conn_pool: SqlConnectionPool,
    rules: List[Dict[str, Any]],
    executed_by: Optional[str] = None,
    max_workers: Optional[int] = None,
    logger: Optional[logging.Logger] = None,
    sample_limit: int = 10,
    batch_size: int = 1000,
    on_rule_complete=None,  # Optional[Callable[[ExecutionResult, Optional[Exception]], None]]
) -> SessionResult:
    """
    Run a list of rules in parallel using ThreadPoolExecutor.
    Each worker borrows a dedicated connection from the pool.
    on_rule_complete(result_or_none, exception_or_none) is called after each rule finishes.
    """
    if max_workers is None:
        max_workers = conn_pool.pool_size if hasattr(conn_pool, 'pool_size') else 4

    # Start a session
    with conn_pool.acquire() as conn:
        session_id = _start_session(conn, executed_by, total_rules=len(rules))
        _set_session_parallelism(conn, session_id, max_workers)
        session_started_at = dt.datetime.utcnow()
        if logger:
            logger.info(f"Session {session_id} started for {len(rules)} rule(s)")

    results: List[ExecutionResult] = []

    def worker(rule_cfg: Dict[str, Any]) -> ExecutionResult:
        with conn_pool.acquire() as conn:
            return execute_rule(conn, rule_cfg, session_id=session_id, logger=logger, sample_limit=sample_limit, batch_size=batch_size)

    futures = {}
    worker_errors = 0
    failed_rule_ids: List[int] = []
    worker_error_details: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for r in rules:
            futures[executor.submit(worker, r)] = r

        for fut in as_completed(futures):
            try:
                res = fut.result()
                results.append(res)
                if res.status != "Succeeded":
                    failed_rule_ids.append(int(res.rule_id))
                if on_rule_complete:
                    try:
                        on_rule_complete(res, None)
                    except Exception:
                        pass
            except Exception as ex:
                worker_errors += 1
                failed_cfg = futures.get(fut) or {}
                failed_rule_id = failed_cfg.get("rule_id")
                if failed_rule_id is not None:
                    failed_rule_ids.append(int(failed_rule_id))
                worker_error_details.append(
                    {
                        "rule_id": int(failed_rule_id) if failed_rule_id is not None else None,
                        "error": str(ex)[:3900],
                    }
                )
                if on_rule_complete:
                    try:
                        on_rule_complete(None, ex)
                    except Exception:
                        pass
                if logger:
                    logger.exception(f"Worker raised: {ex}")

    # Compute aggregates
    passed = sum(1 for r in results if r.status == "Succeeded")
    failed = len(rules) - passed
    errors_count = failed + worker_errors

    with conn_pool.acquire() as conn:
        _update_session_summary(
            conn,
            session_id,
            total_rules=len(rules),
            passed=passed,
            failed=failed,
            errors_count=errors_count,
            failed_rule_ids=failed_rule_ids,
            worker_error_details=worker_error_details,
        )

    session_completed_at = dt.datetime.utcnow()
    duration_ms = int((session_completed_at - session_started_at).total_seconds() * 1000)

    if logger:
        logger.info(
            f"Session {session_id} completed: total={len(rules)} passed={passed} failed={failed} errors={errors_count} duration_ms={duration_ms}"
        )

    return SessionResult(
        session_id=session_id,
        total_rules=len(rules),
        passed=passed,
        failed=failed,
        started_at=session_started_at,
        completed_at=session_completed_at,
        duration_ms=duration_ms,
        rule_results=results,
    )