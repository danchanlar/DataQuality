import sys, os
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
sys.path.insert(0, ROOT)

import streamlit as st
import json

from dq_tool.ui.components.connection_sidebar import connection_sidebar
from dq_tool.ui.utils.paging import render_pagination_controls

st.title("📜 History & Audit")
connection_sidebar()

if "pool" not in st.session_state:
    st.warning("Connect first.")
    st.stop()

pool = st.session_state.pool


def _parse_failed_rule_ids(raw_value):
    if raw_value is None:
        return []
    if isinstance(raw_value, list):
        return [int(x) for x in raw_value if x is not None]
    text = str(raw_value).strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return [int(x) for x in parsed if x is not None]
    except Exception:
        pass
    return []


def _parse_json_list(raw_value):
    if raw_value is None:
        return []
    if isinstance(raw_value, list):
        return raw_value
    text = str(raw_value).strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return parsed
    except Exception:
        pass
    return []

# Sessions
with pool.acquire() as conn:
    cur = conn.cursor()
    sessions = cur.execute("""
        SELECT * FROM DQ.ExecutionSessions
        ORDER BY session_id DESC
    """).fetchall()

# Pagination for sessions
st.divider()
st.markdown("#### 📋 Execution Sessions")
page_sessions, total_pages, current_page = render_pagination_controls(sessions, page_size=20, key="history_audit")
st.divider()

for s in page_sessions:
    with st.expander(f"Session {s.session_id}"):
        started_at = getattr(s, "started_at", None)
        completed_at = getattr(s, "completed_at", None)
        executed_by = getattr(s, "executed_by", None)
        total_rules = getattr(s, "total_rules", None)
        passed = getattr(s, "passed", None)
        failed = getattr(s, "failed", None)
        failed_rule_ids_raw = getattr(s, "failed_rule_ids", None)
        failed_rule_ids = _parse_failed_rule_ids(failed_rule_ids_raw)
        worker_error_details = _parse_json_list(getattr(s, "worker_error_details_json", None))

        # Include worker-level rule ids if present.
        for item in worker_error_details:
            if isinstance(item, dict) and item.get("rule_id") is not None:
                failed_rule_ids.append(int(item["rule_id"]))
        failed_rule_ids = sorted(set(failed_rule_ids))

                # Fallback for older sessions that do not have session-level failed ids populated.
        if not failed_rule_ids and (failed or 0) > 0:
            failed_rows = cur.execute(
                """
                SELECT DISTINCT rule_id
                FROM DQ.RuleExecutions
                                WHERE session_id = ?
                                    AND (
                                                status <> 'Succeeded'
                                         OR error_message IS NOT NULL
                                         OR (violations_count IS NULL AND completed_at IS NOT NULL)
                                    )
                  AND rule_id IS NOT NULL
                ORDER BY rule_id
                """,
                (s.session_id,),
            ).fetchall()
            failed_rule_ids = [int(r.rule_id) for r in failed_rows]

        duration_text = "-"
        if started_at and completed_at:
            duration_text = str(completed_at - started_at)

        c1, c2, c3 = st.columns(3)
        c1.metric("Session ID", s.session_id)
        c2.metric("Executed By", executed_by or "-")
        c3.metric("Duration", duration_text)

        c4, c5, c6 = st.columns(3)
        c4.metric("Total Rules", total_rules if total_rules is not None else "-")
        c5.metric("Passed", passed if passed is not None else "-")
        c6.metric("Failed", failed if failed is not None else "-")

        if failed_rule_ids:
            st.error("Failed Rule IDs: " + ", ".join(str(rid) for rid in failed_rule_ids))
        else:
            st.success("Failed Rule IDs: none")

        if worker_error_details:
            st.caption("Worker-level errors")
            for idx, item in enumerate(worker_error_details, start=1):
                if isinstance(item, dict):
                    wrule = item.get("rule_id")
                    wmsg = item.get("error")
                    st.code(f"{idx}. rule_id={wrule} | error={wmsg}")

        if (failed or 0) > 0 and len(failed_rule_ids) < int(failed or 0):
            st.warning(
                "Some failed executions could not be mapped to rule_id from session data. "
                "This can happen in older runs or worker-level failures."
            )

        st.caption(f"Started: {started_at or '-'}")
        st.caption(f"Completed: {completed_at or '-'}")

        execs = cur.execute("""
            SELECT * 
            FROM DQ.RuleExecutions
            WHERE session_id = ?
            ORDER BY execution_id
        """, (s.session_id,)).fetchall()

        for e in execs:
            with st.expander(f"Execution {e.execution_id} | Rule {e.rule_id} → {e.status}"):
                try:
                    summary = json.loads(e.result_summary)
                except:
                    summary = {"raw": e.result_summary}
                st.json(summary)

