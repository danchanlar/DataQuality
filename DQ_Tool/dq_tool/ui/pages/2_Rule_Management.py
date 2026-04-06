import sys, os
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
sys.path.insert(0, ROOT)

import streamlit as st
import json
import tempfile
from dq_tool.ui.components.connection_sidebar import connection_sidebar
from dq_tool.ui.components.rule_tree import render_rule_tree
from dq_tool.ui.components.results_table import render_results_table
from dq_engine.dq_engine import execute_session
from dq_engine.dq_persistence import import_rules_from_json
from dq_engine.dq_logging import setup_logger
from dq_tool.ui.utils.paging import render_pagination_controls

st.title("📂 Rule Management")
connection_sidebar()

# Check connection
if "pool" not in st.session_state:
    st.warning("Connect first.")
    st.stop()

pool = st.session_state.pool

# Load rules
with pool.acquire() as conn:
    cur = conn.cursor()
    rules = cur.execute("""
        SELECT rule_id, rule_type, target_schema, target_table, target_column,
               parameters_json, is_active
        FROM DQ.Rules
        ORDER BY target_schema, target_table, rule_id
    """).fetchall()

# Filters for large rule sets
all_schemas = sorted({r.target_schema for r in rules if r.target_schema})
all_tables = sorted({f"{r.target_schema}.{r.target_table}" for r in rules if r.target_schema and r.target_table})

st.caption(f"Loaded rules: {len(rules)} | Tables: {len(all_tables)}")

fcol1, fcol2 = st.columns([1, 2])
with fcol1:
    selected_schemas = st.multiselect("Filter schemas", all_schemas)
with fcol2:
    search_text = st.text_input(
        "Search table/column/rule type",
        placeholder="e.g. dbo.lgm or NOT_NULL",
    ).strip().lower()

filtered_rules = []
for r in rules:
    if selected_schemas and r.target_schema not in selected_schemas:
        continue

    hay = f"{r.target_schema}.{r.target_table} {r.target_column or ''} {r.rule_type or ''}".lower()
    if search_text and search_text not in hay:
        continue

    filtered_rules.append(r)

filtered_tables = len({f"{r.target_schema}.{r.target_table}" for r in filtered_rules if r.target_schema and r.target_table})
st.caption(f"Showing rules: {len(filtered_rules)} | Tables: {filtered_tables}")

st.markdown("---")
with st.expander("Bulk Admin (Current Filters)", expanded=False):
    st.caption(
        f"Current scope: {len(filtered_rules)} rules across {filtered_tables} tables"
    )

    col_exp, col_imp = st.columns(2)

    with col_exp:
        st.markdown("**Export Filtered Rules**")
        if st.button("Prepare Export JSON", key="bulk_export_prepare", use_container_width=True):
            export_payload = []
            for r in filtered_rules:
                try:
                    params = json.loads(r.parameters_json) if r.parameters_json else {}
                except Exception:
                    params = {}

                export_payload.append(
                    {
                        "rule_id": int(r.rule_id),
                        "rule_type": r.rule_type,
                        "target_schema": r.target_schema,
                        "target_table": r.target_table,
                        "target_column": r.target_column,
                        "parameters": params,
                        "is_active": bool(r.is_active),
                    }
                )

            st.session_state["bulk_filtered_export_json"] = json.dumps(
                export_payload,
                ensure_ascii=False,
                indent=2,
            )
            st.success(f"Prepared {len(export_payload)} rules for download.")

        if st.session_state.get("bulk_filtered_export_json"):
            st.download_button(
                label="Download Filtered rules.json",
                data=st.session_state["bulk_filtered_export_json"],
                file_name="rules_filtered_export.json",
                mime="application/json",
                key="bulk_export_download",
                use_container_width=True,
            )

    with col_imp:
        st.markdown("**Import Rules (Bulk)**")
        uploaded_bulk = st.file_uploader(
            "Upload JSON file",
            type=["json"],
            key="bulk_import_file",
        )

        overwrite_existing = st.checkbox(
            "Overwrite duplicates",
            value=False,
            key="bulk_import_overwrite",
        )
        dry_run_only = st.checkbox(
            "Dry run only (preview, no DB changes)",
            value=True,
            key="bulk_import_dry_run",
        )

        parsed_bulk_items = None
        if uploaded_bulk is not None:
            try:
                parsed_bulk_items = json.load(uploaded_bulk)
                if not isinstance(parsed_bulk_items, list):
                    st.error("Invalid JSON format: expected a list of rules.")
                    parsed_bulk_items = None
                else:
                    preview_tables = {
                        f"{it.get('target_schema')}.{it.get('target_table')}"
                        for it in parsed_bulk_items
                        if it.get("target_schema") and it.get("target_table")
                    }
                    st.info(
                        f"File contains {len(parsed_bulk_items)} rules across {len(preview_tables)} tables."
                    )
            except Exception as exc:
                st.error(f"Could not read JSON file: {exc}")

        if st.button("Apply Import", key="bulk_import_apply", use_container_width=True):
            if parsed_bulk_items is None:
                st.error("Upload a valid JSON file first.")
            elif dry_run_only:
                st.success(
                    f"Dry run OK: {len(parsed_bulk_items)} rules would be imported "
                    f"(overwrite={'ON' if overwrite_existing else 'OFF'})."
                )
            else:
                temp_path = None
                try:
                    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8") as tmp:
                        json.dump(parsed_bulk_items, tmp, ensure_ascii=False, indent=2)
                        temp_path = tmp.name

                    inserted = import_rules_from_json(pool, temp_path, overwrite=overwrite_existing)
                    st.success(f"Imported {inserted} rules.")
                    st.rerun()
                except Exception as exc:
                    st.error(f"Import failed: {exc}")
                finally:
                    if temp_path and os.path.exists(temp_path):
                        os.remove(temp_path)

    st.markdown("**Bulk State Changes (Filtered Scope)**")
    bulk_action = st.selectbox(
        "Action",
        ["Enable filtered rules", "Disable filtered rules", "Delete filtered rules (danger)"],
        key="bulk_state_action",
    )
    ack_bulk = st.checkbox(
        "I understand this will affect all currently filtered rules",
        key="bulk_ack",
    )
    ack_text = st.text_input(
        "Type APPLY to confirm",
        key="bulk_ack_text",
    ).strip()

    if st.button("Run Bulk Action", key="bulk_apply_action", use_container_width=True):
        if not filtered_rules:
            st.warning("No filtered rules in scope.")
        elif not ack_bulk or ack_text != "APPLY":
            st.error("Confirmation failed. Tick the checkbox and type APPLY.")
        else:
            ids = [int(r.rule_id) for r in filtered_rules]
            with pool.acquire() as conn:
                cur = conn.cursor()
                if bulk_action == "Enable filtered rules":
                    for rid in ids:
                        cur.execute("UPDATE DQ.Rules SET is_active = 1 WHERE rule_id=?", (rid,))
                    st.success(f"Enabled {len(ids)} rules.")
                elif bulk_action == "Disable filtered rules":
                    for rid in ids:
                        cur.execute("UPDATE DQ.Rules SET is_active = 0 WHERE rule_id=?", (rid,))
                    st.success(f"Disabled {len(ids)} rules.")
                else:
                    for rid in ids:
                        cur.execute(
                            """
                            DELETE FROM DQ.RuleViolations
                            WHERE execution_id IN (
                                SELECT execution_id FROM DQ.RuleExecutions WHERE rule_id=?
                            )
                            """,
                            (rid,),
                        )
                        cur.execute("DELETE FROM DQ.RuleExecutions WHERE rule_id=?", (rid,))
                        cur.execute("DELETE FROM DQ.Rules WHERE rule_id=?", (rid,))
                    st.warning(f"Deleted {len(ids)} rules and related history.")
            st.rerun()

# Pagination
st.divider()
st.markdown("#### 📃 Rule List")
page_rules, total_pages, current_page = render_pagination_controls(filtered_rules, page_size=50, key="rule_management")

st.divider()

# Tree view with action signals
actions = render_rule_tree(page_rules)

# Process actions
for action, rule_id in actions:
    if action == "run":
        target_rule = None
        for r in filtered_rules:
            if r.rule_id == rule_id:
                target_rule = {
                    "rule_id": r.rule_id,
                    "rule_type": r.rule_type,
                    "target_schema": r.target_schema,
                    "target_table": r.target_table,
                    "target_column": r.target_column,
                    "parameters": json.loads(r.parameters_json) if r.parameters_json else {},
                }
                break

        if target_rule is None:
            st.error(f"Rule {rule_id} not found.")
            st.stop()

        logger = setup_logger("dq_ui.log")
        result = execute_session(
            pool,
            [target_rule],
            executed_by="Danai",
            max_workers=1,
            logger=logger,
        )
        st.success(f"Rule {rule_id} executed in session {result.session_id}.")
        render_results_table(result)
        st.stop()

    with pool.acquire() as conn:
        cur = conn.cursor()
        if action == "disable":
            cur.execute("UPDATE DQ.Rules SET is_active = 0 WHERE rule_id=?", (rule_id,))
            st.success(f"Rule {rule_id} disabled.")
        elif action == "enable":
            cur.execute("UPDATE DQ.Rules SET is_active = 1 WHERE rule_id=?", (rule_id,))
            st.success(f"Rule {rule_id} enabled.")
        elif action == "delete":
            # Dangerous: delete history also
            cur.execute("""
                DELETE FROM DQ.RuleViolations 
                WHERE execution_id IN (SELECT execution_id FROM DQ.RuleExecutions WHERE rule_id=?)
            """, (rule_id,))
            cur.execute("DELETE FROM DQ.RuleExecutions WHERE rule_id=?", (rule_id,))
            cur.execute("DELETE FROM DQ.Rules WHERE rule_id=?", (rule_id,))
            st.warning(f"Rule {rule_id} permanently deleted.")
    st.rerun()

