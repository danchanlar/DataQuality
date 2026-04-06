
import streamlit as st
import pandas as pd

def render_results_table(session_result):
    """Display the results of execute_session()."""

    rows = []
    for r in session_result.rule_results:
        rows.append({
            "Rule ID": r.rule_id,
            "Status": r.status,
            "Violations": r.violations_count,
            "Duration (ms)": r.duration_ms
        })

    df = pd.DataFrame(rows)

    st.subheader("Execution Results")
    st.dataframe(df, use_container_width=True)

    # Expand rule details
    for r in session_result.rule_results:
        with st.expander(f"Rule {r.rule_id} → {r.status}"):
            st.write(f"Duration: {r.duration_ms} ms")
            st.json(r.sample_violations)
            if r.error_message:
                st.error(r.error_message)

