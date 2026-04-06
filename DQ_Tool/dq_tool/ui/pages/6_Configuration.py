import sys, os
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
sys.path.insert(0, ROOT)

import streamlit as st
from dq_engine.db_connection import SqlConnectionPool


def _sync_exec_settings_from_widgets():
    st.session_state.exec_max_workers = int(st.session_state.exec_max_workers_widget)
    # Keep pool size derived from workers to avoid over-allocating idle connections.
    st.session_state.pool_size = st.session_state.exec_max_workers + 2
    st.session_state.pool_size_widget = st.session_state.pool_size

st.set_page_config(page_title="Configuration", layout="wide")
st.title("⚙️ Configuration")

# ============================================================================
# PROGRAM DATABASE INFO
# ============================================================================

st.info("""
📊 **Program Database**: This tool uses the **DQ** schema in your connected database.
All rules, executions, and violations are stored in the DQ database schema.
""")

st.subheader("📁 External SQL Source")

if "sql_root_path" not in st.session_state:
    st.session_state.sql_root_path = r"C:\Users\d.chalandrinou\Desktop\CSB2DATA"

sql_root_path = st.text_input(
    "External SQL Root Path",
    key="sql_root_path",
    help="Path to the SQL repository used by Auto Discovery (read-only).",
)

if sql_root_path and os.path.isdir(sql_root_path):
    st.session_state["discovery_repo_path"] = sql_root_path
    st.success(f"SQL source ready: {sql_root_path}")
else:
    st.warning("Set a valid SQL root folder. Auto Discovery uses this path.")

st.divider()

if "conn_server" not in st.session_state:
    st.session_state.conn_server = "SANDBOX-SQL\\MSSQL2022"
if "conn_database" not in st.session_state:
    st.session_state.conn_database = "CSBDATA_DEV"
if "conn_auth_type" not in st.session_state:
    st.session_state.conn_auth_type = "Windows Authentication"
if "conn_username" not in st.session_state:
    st.session_state.conn_username = ""
if "conn_password" not in st.session_state:
    st.session_state.conn_password = ""
if "exec_max_workers" not in st.session_state:
    st.session_state.exec_max_workers = 5
if "pool_size" not in st.session_state:
    st.session_state.pool_size = st.session_state.exec_max_workers + 2

# Keep widget state separate from persistent state so values survive page switches.
if "exec_max_workers_widget" not in st.session_state:
    st.session_state.exec_max_workers_widget = st.session_state.exec_max_workers
if "pool_size_widget" not in st.session_state:
    st.session_state.pool_size_widget = st.session_state.pool_size

# Enforce invariant: pool_size = max_workers + 2
desired_pool_size = st.session_state.exec_max_workers + 2
if st.session_state.pool_size != desired_pool_size:
    st.session_state.pool_size = desired_pool_size
if st.session_state.pool_size_widget != desired_pool_size:
    st.session_state.pool_size_widget = desired_pool_size

# Check if reset was requested, apply BEFORE creating widgets with those keys
if st.session_state.get("_reset_requested", False):
    st.session_state.exec_max_workers = 5
    st.session_state.pool_size = 7
    st.session_state.exec_max_workers_widget = 5
    st.session_state.pool_size_widget = 7
    st.session_state._reset_requested = False

st.divider()

# ============================================================================
# CONNECTION CONFIGURATION
# ============================================================================

st.subheader("🔌 Database Connection")

c1, c2 = st.columns(2)

with c1:
    st.text_input("Server", key="conn_server")
with c2:
    st.text_input("Database", key="conn_database")

c3, c4 = st.columns(2)

with c3:
    auth_type = st.selectbox(
        "Authentication",
        ["Windows Authentication", "SQL Server Authentication"],
        key="conn_auth_type",
    )

with c4:
    if auth_type == "SQL Server Authentication":
        st.text_input("Username", key="conn_username")
        st.text_input("Password", type="password", key="conn_password")

# Connection status and actions
conn_col1, conn_col2, conn_col3 = st.columns([2, 1, 1])

with conn_col1:
    if "pool" in st.session_state:
        active_pool = st.session_state.pool
        st.success(
            f"✅ Connected: {active_pool.server} / {active_pool.database}"
        )
    #else:
        #st.warning("❌ Not connected")

with conn_col2:
    if st.button("Connect", use_container_width=True):
        try:
            username = st.session_state.conn_username if auth_type == "SQL Server Authentication" else ""
            password = st.session_state.conn_password if auth_type == "SQL Server Authentication" else ""

            st.session_state.pool = SqlConnectionPool(
                server=st.session_state.conn_server,
                database=st.session_state.conn_database,
                auth_type=auth_type,
                username=username,
                password=password,
            )
            st.success("Connected successfully!")
            st.rerun()
        except Exception as e:
            st.error(f"Connection failed: {e}")

with conn_col3:
    if "pool" in st.session_state:
        if st.button("Disconnect", use_container_width=True):
            try:
                st.session_state.pool.close_all()
            except Exception:
                pass
            st.session_state.pop("pool", None)
            st.success("Disconnected")
            st.rerun()

st.divider()

# ============================================================================
# EXECUTION SETTINGS
# ============================================================================

st.subheader("⚡ Execution Settings")

e1, e2 = st.columns(2)

with e1:
    st.slider(
        "Max Workers (Parallelism)",
        min_value=1,
        max_value=20,
        step=1,
        help="Number of concurrent worker threads for rule execution",
        key="exec_max_workers_widget",
        on_change=_sync_exec_settings_from_widgets,
    )

with e2:
    st.metric(
        "Connection Pool Size",
        value=st.session_state.pool_size,
        help="Auto-calculated as max_workers + 2",
    )
    st.caption("Auto rule: pool_size = max_workers + 2")

st.divider()

# ============================================================================
# VALIDATION
# ============================================================================

st.subheader("Configuration Validation")

pool_size = st.session_state.pool_size
max_workers = st.session_state.exec_max_workers

expected_pool_size = max_workers + 2

if pool_size == expected_pool_size:
    st.success(
        f"✅ Configuration is valid: pool_size ({pool_size}) = max_workers ({max_workers}) + 2"
    )
else:
    st.warning(f"⚠️ pool_size adjusted to {expected_pool_size} to match max_workers + 2.")

st.markdown("""
---

### Setting Guidelines

**Max Workers**
- Number of concurrent threads executing rules
- Higher = faster, but requires more resources
- Safe range: 1-20

**Connection Pool Size**
- Automatically calculated from workers
- Formula: `pool_size = max_workers + 2`
- Extra 2 connections provide small safety margin

**Policy**: pool_size is fixed to max_workers + 2
- Example: max_workers=8 → pool_size=10

""")


if st.button("Reset Execution Settings to Defaults", use_container_width=True):
    st.session_state._reset_requested = True
    st.rerun()



