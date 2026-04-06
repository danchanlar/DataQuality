import streamlit as st
from dq_engine.sql_generators import generate_sql

def render_sql_preview(rule):
    st.subheader("SQL Preview")

    try:
        sql_preview, sql_params = generate_sql(rule)
        st.code(sql_preview, language="sql")
        st.json({"params": sql_params})
    except Exception as e:
        st.error(f"SQL generation error: {e}")

