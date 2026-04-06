import sys, os
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
sys.path.insert(0, ROOT)

import streamlit as st
import json

from dq_tool.ui.components.connection_sidebar import connection_sidebar
from dq_tool.ui.components.rule_form import render_rule_form
from dq_tool.ui.components.sql_preview import render_sql_preview
from dq_engine.dq_persistence import export_rules_to_json, import_rules_from_json


def _count_sql_placeholders(sql_text: str) -> int:
    """Count ? placeholders while ignoring quoted string literals."""
    if not sql_text:
        return 0

    in_single_quote = False
    i = 0
    count = 0
    while i < len(sql_text):
        ch = sql_text[i]
        if ch == "'":
            # SQL escaped single quote inside string: ''
            if in_single_quote and i + 1 < len(sql_text) and sql_text[i + 1] == "'":
                i += 2
                continue
            in_single_quote = not in_single_quote
            i += 1
            continue

        if ch == "?" and not in_single_quote:
            count += 1
        i += 1

    return count

# validiation jelper for custom SQL rules - ensuring parameter count matches placeholders
def _validate_custom_sql_params(rule_params: dict) -> tuple[bool, str, int, int]:
    params = rule_params.get("params", [])
    if not isinstance(params, list):
        return False, "Query parameters must be a JSON array.", 0, 0

    custom_sql = (rule_params.get("sql") or "").strip()
    where_clause = (rule_params.get("where") or "").strip()
    sql_for_count = custom_sql if custom_sql else where_clause

    if not sql_for_count:
        return False, "Provide either a WHERE predicate or a full SQL query.", 0, len(params)

    placeholder_count = _count_sql_placeholders(sql_for_count)
    param_count = len(params)
    if placeholder_count != param_count:
        return (
            False,
            f"Placeholder mismatch: found {placeholder_count} '?' placeholders but {param_count} parameters.",
            placeholder_count,
            param_count,
        )

    return True, "", placeholder_count, param_count

st.title("📘 Rule Definition")

# Sidebar Connection
connection_sidebar()

# Check connection
if "pool" not in st.session_state:
    st.warning("Connect to the database from the sidebar first.")
    st.stop()

pool = st.session_state.pool
st.caption(f"Connected to: {pool.server} / {pool.database}")

# ---- 1. Load tables ----
with pool.acquire() as conn:
    cur = conn.cursor()
    cur.execute("""
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_SCHEMA, TABLE_NAME
    """)
    tables = [f"{r.TABLE_SCHEMA}.{r.TABLE_NAME}" for r in cur.fetchall()]

table = st.selectbox("Select Table", tables)

if not table:
    st.stop()

schema, table_name = table.split(".")

# 2. Load columns
with pool.acquire() as conn:
    cur = conn.cursor()
    cur.execute("""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
    """, (schema, table_name))
    columns = [r.COLUMN_NAME for r in cur.fetchall()]

# 3. Rule Type
rule_type = st.selectbox(
    "Rule Type",
    [
        # Core types
        "NOT_NULL",
        "VALUE_RANGE",
        "STRING_LENGTH",
        "PATTERN_MATCH",
        "UNIQUENESS",
        "ALLOWED_VALUES",
        "DATE_RANGE",
        "CROSS_COLUMN",
        "CONDITIONAL_REQUIRED",
        "DUPLICATE_ROWS",
        # Advanced types
        "REFERENTIAL_INTEGRITY",
        "ORPHANED_ROWS",
        "COMPLETENESS",
        "NEGATIVE_VALUES",
        "NON_NEGATIVE_VALUES",
        "POSITIVE_VALUES",
        "REGEX_MATCH",
        "ROW_COUNT_MIN",
        # Custom
        "CUSTOM_SQL_FILTER",
    ]
)

# 4. Column selection (rule-aware) 
if rule_type == "NOT_NULL":
    col = st.selectbox("Select Column", columns)
else:
    col = st.selectbox("Select Column", ["<table_level>"] + columns)

#5. Parameters (from component)
params = render_rule_form(rule_type, columns)

# 6. Build the rule dict
rule = {
    "rule_id": None,
    "rule_type": rule_type,
    "target_schema": schema,
    "target_table": table_name,
    "target_column": None if col == "<table_level>" else col,
    "parameters": params
}

save_validation_error = None
if rule_type == "CUSTOM_SQL_FILTER":
    is_valid, message, ph_count, p_count = _validate_custom_sql_params(params)
    st.caption(f"Placeholder check: placeholders={ph_count}, params={p_count}")
    if not is_valid:
        save_validation_error = message
        st.error(message)
    else:
        st.success("Custom SQL parameters are aligned with placeholders.")

#7. SQL Preview (component)
render_sql_preview(rule)

#8. Save Rule
if st.button("Save Rule", disabled=bool(save_validation_error)):
    with pool.acquire() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO DQ.Rules (
                rule_type, target_schema, target_table, target_column,
                parameters_json, is_active, created_by
            )
            OUTPUT INSERTED.rule_id
            VALUES (?, ?, ?, ?, ?, 1, SUSER_SNAME())
        """, (
            rule_type,
            schema,
            table_name,
            None if col == "<table_level>" else col,
            json.dumps(params)
        ))
        inserted_rule_id = int(cur.fetchone()[0])

        cur.execute(
            "SELECT COUNT(1) AS cnt FROM DQ.Rules WHERE rule_id = ?",
            (inserted_rule_id,),
        )
        exists = int(cur.fetchone()[0]) == 1

    if exists:
        st.success(f"Rule saved in database successfully (rule_id={inserted_rule_id}).")
    else:
        st.error("Insert completed but verification failed. Please check DB permissions/transactions.")

# ============================================================================
# Export / Import Quick Actions
# ============================================================================
st.markdown("---")
st.subheader("📥📤 Quick Export / Import")

col_exp, col_imp = st.columns(2)

with col_exp:
    st.markdown("**Download all rules as JSON**")
    if st.button("📥 Export Rules", use_container_width=True):
        try:
            json_str = export_rules_to_json(pool, "_temp_export.json")
            with open("_temp_export.json", "r", encoding="utf-8") as f:
                json_data = f.read()
            
            st.download_button(
                label="⬇️ Download rules.json",
                data=json_data,
                file_name="rules_export.json",
                mime="application/json",
                use_container_width=True
            )
            st.success(f"✅ Exported {json_str} rules")
            
            # Clean up temp file
            import os
            if os.path.exists("_temp_export.json"):
                os.remove("_temp_export.json")
                
        except Exception as exc:
            st.error(f"Export failed: {exc}")

with col_imp:
    st.markdown("**Upload rules from JSON**")
    uploaded_file = st.file_uploader(
        "Select JSON file",
        type=["json"],
        key="rule_import",
        label_visibility="collapsed"
    )
    
    if uploaded_file:
        try:
            imported_items = json.load(uploaded_file)
            st.info(f"📦 File has {len(imported_items)} rules")
            
            overwrite = st.checkbox(
                "Overwrite duplicates?",
                value=False,
                key="import_overwrite"
            )
            
            if st.button("📤 Import Rules", use_container_width=True):
                try:
                    temp_path = "_temp_import.json"
                    with open(temp_path, 'w', encoding='utf-8') as f:
                        json.dump(imported_items, f, ensure_ascii=False, indent=2)
                    
                    inserted = import_rules_from_json(pool, temp_path, overwrite=overwrite)
                    st.success(f"✅ Imported {inserted} new rules")
                    
                    # Clean up
                    import os
                    if os.path.exists(temp_path):
                        os.remove(temp_path)
                        
                except Exception as exc:
                    st.error(f"Import failed: {exc}")
        
        except json.JSONDecodeError:
            st.error("❌ Invalid JSON file")
        except Exception as exc:
            st.error(f"Error reading file: {exc}")

