import sys, os
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
sys.path.insert(0, ROOT)

import time
import streamlit as st
from datetime import datetime
from dq_tool.ui.utils.execution_logger import (
    get_execution_log_content,
    clear_execution_log,
    EXECUTION_LOG_FILE,
)
from dq_tool.ui.components.connection_sidebar import connection_sidebar

st.title("📋 Execution Logs (Live)")
connection_sidebar()

st.markdown("""
Monitor real-time execution of Data Quality rules. This page reads from the execution log
to show you when each rule starts, completes, or fails.
""")

st.divider()

# Auto-refresh section
col1, col2, col3 = st.columns(3)

with col1:
    auto_refresh = st.checkbox("Auto-refresh every 2 seconds", value=True)
    #if auto_refresh:
       # st.write("🔄 Refreshing...")

with col2:
    lines_to_show = st.slider("Show last N lines", min_value=10, max_value=500, value=100, step=10)

with col3:
    if st.button("Clear Log", use_container_width=True):
        try:
            clear_execution_log()
            st.success("Execution log cleared!")
            st.rerun()
        except Exception as e:
            st.error(f"Could not clear log: {e}")

st.divider()

# Display log file status
if EXECUTION_LOG_FILE.exists():
    file_size = EXECUTION_LOG_FILE.stat().st_size
    file_mtime = datetime.fromtimestamp(EXECUTION_LOG_FILE.stat().st_mtime)
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Log File Size", f"{file_size / 1024:.1f} KB")
    with col2:
        st.metric("Last Updated", file_mtime.strftime("%H:%M:%S"))
    with col3:
        st.metric("File Path", EXECUTION_LOG_FILE.name)
else:
    st.warning("⚠️ Execution log file does not exist yet. Run some rules first!")

st.divider()

# Main log display
st.subheader("📝 Log Content")

log_content = get_execution_log_content(last_n_lines=lines_to_show)

# Display with fixed-width font for better readability
st.code(log_content, language="")

# Auto-refresh with throttle to avoid hammering
if auto_refresh:
    time.sleep(2)
    st.rerun()


