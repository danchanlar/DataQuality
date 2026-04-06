
import sys, os
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
sys.path.insert(0, ROOT)

import streamlit as st


def connection_sidebar():
    """Display connection status in sidebar. Full connection form is in Configuration page."""
    with st.sidebar:
        st.markdown("### 🔌 Database Connection")
        
        # Validate execution sizing policy
        max_workers = st.session_state.get("exec_max_workers", 5)
        pool_size = st.session_state.get("pool_size", max_workers + 2)
        expected_pool_size = max_workers + 2
        
        if pool_size != expected_pool_size:
            st.warning(
                f"⚠️ Pool size ({pool_size}) is not max_workers + 2 ({expected_pool_size}). "
                f"Configure in ⚙️ Configuration page."
            )
        
        # Display connection status
        if "pool" in st.session_state:
            active_pool = st.session_state.pool
            st.success(f"✅ Connected: {active_pool.server} / {active_pool.database}")
            
            col1, col2 = st.columns(2)
            with col1:
                if st.button("Change", use_container_width=True):
                    st.session_state.pop("pool", None)
                    st.rerun()
            
            with col2:
                if st.button("Disconnect", use_container_width=True):
                    try:
                        st.session_state.pool.close_all()
                    except Exception:
                        pass
                    st.session_state.pop("pool", None)
                    st.rerun()
        else:
            st.error("❌ Not connected")
            st.info("Go to Configuration page to connect")
        
        st.divider()

