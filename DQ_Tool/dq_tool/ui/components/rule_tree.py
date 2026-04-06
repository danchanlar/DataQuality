import streamlit as st
import json
from collections import defaultdict

def render_rule_tree(rules):
    """Render a tree view of rules grouped by table/column."""

    grouped = defaultdict(lambda: defaultdict(list))

    for r in rules:
        grouped[f"{r.target_schema}.{r.target_table}"][r.target_column].append(r)

    for table, columns in grouped.items():
        with st.expander(f"📄 {table}"):
            for col, col_rules in columns.items():
                col_name = col if col else "<table-level>"
                st.markdown(f"#### 🔸 Column: **{col_name}**")

                for r in col_rules:
                    with st.expander(f"[{r.rule_id}] {r.rule_type}"):
                        try:
                            parameters = json.loads(r.parameters_json) if r.parameters_json else {}
                        except Exception:
                            parameters = {"_parse_error": "Invalid parameters_json"}

                        rule_definition = {
                            "rule_id": r.rule_id,
                            "rule_type": r.rule_type,
                            "target_schema": r.target_schema,
                            "target_table": r.target_table,
                            "target_column": r.target_column,
                            "is_active": bool(r.is_active),
                            "parameters": parameters,
                        }

                        st.markdown("**Rule definition**")
                        st.json(rule_definition)
                        st.caption("Status: Active" if bool(r.is_active) else "Status: Inactive")

                        if st.button(f"Run {r.rule_id}", key=f"run_{r.rule_id}"):
                            yield ("run", r.rule_id)
                        if st.button(f"Delete {r.rule_id}", key=f"del_{r.rule_id}"):
                            yield ("delete", r.rule_id)
                        if bool(r.is_active):
                            if st.button(f"Disable {r.rule_id}", key=f"disable_{r.rule_id}"):
                                yield ("disable", r.rule_id)
                        else:
                            if st.button(f"Enable {r.rule_id}", key=f"enable_{r.rule_id}"):
                                yield ("enable", r.rule_id)

