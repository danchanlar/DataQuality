import sys, os
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
sys.path.insert(0, ROOT)

import streamlit as st
import json
import pandas as pd
import time
import threading

from dq_tool.ui.components.connection_sidebar import connection_sidebar
from dq_tool.ui.components.results_table import render_results_table

from dq_engine.dq_engine import execute_session
from dq_engine.dq_logging import setup_logger
from dq_engine.sql_generators import generate_sql
from dq_tool.ui.utils.execution_logger import (
    log_execution_start,
    log_rule_execution,
    log_execution_end,
    log_error,
)
from dq_tool.ui.utils.execution_state import ExecutionProgress
from dq_tool.ui.utils.paging import render_pagination_controls

st.title("⚡ Execution Dashboard")
connection_sidebar()

if "pool" not in st.session_state:
    st.warning("Connect first.")
    st.stop()

pool = st.session_state.pool

# Load active rules
with pool.acquire() as conn:
    cur = conn.cursor()
    rules = cur.execute("""
        SELECT rule_id, rule_type, target_schema, target_table, 
               target_column, parameters_json
        FROM DQ.Rules
        WHERE is_active = 1
        ORDER BY rule_id
    """).fetchall()

rule_ids = [r.rule_id for r in rules]

st.subheader("Active Rules (preview before execution)")
if not rules:
    st.info("No active rules found in DQ.Rules.")
    st.stop()

schemas = sorted({r.target_schema for r in rules})
tables = sorted({f"{r.target_schema}.{r.target_table}" for r in rules})
rule_types = sorted({r.rule_type for r in rules})

fcol1, fcol2, fcol3 = st.columns(3)
with fcol1:
    selected_schemas = st.multiselect("Filter by schema", schemas)
with fcol2:
    selected_tables = st.multiselect("Filter by table", tables)
with fcol3:
    selected_rule_types = st.multiselect("Filter by rule type", rule_types)

search_text = st.text_input(
    "Search (rule id, table, column)",
    placeholder="e.g. 12 or Customers or Email",
)

filtered_rules = []
for r in rules:
    table_name = f"{r.target_schema}.{r.target_table}"
    if selected_schemas and r.target_schema not in selected_schemas:
        continue
    if selected_tables and table_name not in selected_tables:
        continue
    if selected_rule_types and r.rule_type not in selected_rule_types:
        continue

    if search_text:
        hay = f"{r.rule_id} {r.target_schema} {r.target_table} {r.target_column or ''} {r.rule_type}".lower()
        if search_text.lower() not in hay:
            continue

    filtered_rules.append(r)

st.caption(f"Showing {len(filtered_rules)} of {len(rules)} active rules")

# Pagination for preview
st.divider()
st.markdown("#### 📋 Rules Preview")
page_rules, total_pages, current_page = render_pagination_controls(filtered_rules, page_size=20, key="execution_dashboard")
st.divider()

preview_rows = []
for r in page_rules:
    try:
        parsed_params = json.loads(r.parameters_json) if r.parameters_json else {}
    except Exception:
        parsed_params = {"_parse_error": "Invalid parameters_json"}

    preview_rows.append(
        {
            "rule_id": r.rule_id,
            "rule_type": r.rule_type,
            "target": f"{r.target_schema}.{r.target_table}",
            "target_column": r.target_column,
            "parameters": json.dumps(parsed_params, ensure_ascii=True),
        }
    )

st.dataframe(pd.DataFrame(preview_rows), use_container_width=True)

for r in page_rules:
    rule_payload = {
        "rule_id": r.rule_id,
        "rule_type": r.rule_type,
        "target_schema": r.target_schema,
        "target_table": r.target_table,
        "target_column": r.target_column,
        "parameters": json.loads(r.parameters_json) if r.parameters_json else {},
    }

    with st.expander(f"Rule {r.rule_id} | {r.rule_type} | {r.target_schema}.{r.target_table}"):
        st.markdown("**Rule payload**")
        st.json(rule_payload)

        try:
            sql_text, sql_params = generate_sql(rule_payload)
            st.markdown("**Generated SQL (violations query)**")
            st.code(sql_text, language="sql")
            st.markdown("**SQL params**")
            st.json(sql_params)
        except Exception as exc:
            st.error(f"Could not generate SQL preview: {exc}")

# Rule selection remains, now followed by dual-button layout.
selected = st.multiselect("Select Rules to Run", rule_ids)

# Initialize configuration defaults if not set
st.session_state.setdefault("exec_max_workers", 5)
st.session_state.setdefault("pool_size", st.session_state.exec_max_workers + 2)

# Use max_workers from Configuration page
max_workers = st.session_state.exec_max_workers

st.info(
    f"📋 Parallelism (max_workers): **{max_workers}** | Pool Size: **{st.session_state.pool_size}** | "
    f"Configure these settings in Configuration page"
)

# Added 2-column button section.
col1, col2 = st.columns(2)

# Run Selected Rules button moved into column layout.
with col1:
    run_selected = st.button("Run Selected Rules", use_container_width=True)

# Unified execution branch now handles both buttons.
with col2:
    run_all = st.button("Run All (Filtered)", use_container_width=True)

# New path to execute all currently filtered rules
if run_selected or run_all:
    rules_to_run = []

    if run_all:
        for r in filtered_rules:
            parsed = json.loads(r.parameters_json) if r.parameters_json else {}
            rules_to_run.append({
                "rule_id": r.rule_id,
                "rule_type": r.rule_type,
                "target_schema": r.target_schema,
                "target_table": r.target_table,
                "target_column": r.target_column,
                "parameters": parsed,
            })
    else:
        for r in filtered_rules:
            if r.rule_id in selected:
                parsed = json.loads(r.parameters_json) if r.parameters_json else {}
                rules_to_run.append({
                    "rule_id": r.rule_id,
                    "rule_type": r.rule_type,
                    "target_schema": r.target_schema,
                    "target_table": r.target_table,
                    "target_column": r.target_column,
                    "parameters": parsed,
                })

    if not rules_to_run:
        st.warning("No rules to execute. Check your filters or select rules.")
        st.stop()

    # Start background execution
    progress = ExecutionProgress(total=len(rules_to_run))
    st.session_state["_exec_progress"] = progress

    log_execution_start(
        rule_id=f"{len(rules_to_run)} rules",
        rule_type="Bulk Execution",
        target_table="Multiple",
        max_workers=max_workers,
        pool_size=st.session_state.pool_size,
    )

    def _background_run(pool, rules, mw, prog):
        logger = setup_logger("dq_ui.log")
        try:
            result = execute_session(
                pool,
                rules,
                executed_by="Danai",
                max_workers=mw,
                logger=logger,
                on_rule_complete=prog.rule_done,
            )
            violation_count = len(result.all_violations) if hasattr(result, "all_violations") else 0
            log_execution_end(len(rules), violation_count, (time.monotonic()))
            prog.mark_done(result=result)
        except Exception as e:
            prog.mark_done(error=str(e))

    t = threading.Thread(
        target=_background_run,
        args=(pool, rules_to_run, max_workers, progress),
        daemon=True,
    )
    t.start()

# ── Live progress display ─────────────────────────────────────────────────────
progress: ExecutionProgress = st.session_state.get("_exec_progress")

if progress is not None:
    snap = progress.snapshot()

    if not snap["done"]:
        pct = snap["completed"] / snap["total"] if snap["total"] else 0
        elapsed = snap["elapsed"]

        st.divider()
        st.subheader("⏳ Execution in progress...")

        col1, col2, col3 = st.columns(3)
        col1.metric("Completed", f"{snap['completed']} / {snap['total']}")
        col2.metric("Failed", snap["failed"])
        col3.metric("Elapsed", f"{elapsed:.0f}s")

        st.progress(pct, text=f"{snap['completed']} of {snap['total']} rules done")

        with st.spinner("Calculating... (auto-refreshes every 2s)"):
            time.sleep(2)
        st.rerun()

    else:
        st.divider()
        if snap["error"]:
            st.error(f"Execution failed: {snap['error']}")
        else:
            result = snap["result"]
            elapsed = snap["elapsed"]
            st.success(
                f"✅ Session {result.session_id} completed in **{elapsed:.1f}s** "
                f"| {snap['total']} rules | {snap['failed']} failed"
            )
            render_results_table(result)

        # Clear progress so page doesn't keep showing old results on next load
        if st.button("Clear results"):
            st.session_state.pop("_exec_progress", None)
            st.rerun()

