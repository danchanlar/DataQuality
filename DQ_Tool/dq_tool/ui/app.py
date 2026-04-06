import sys, os
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT)

import streamlit as st

st.set_page_config(page_title="DQ Tool", layout="wide")

st.title("🔍 DQ Tool - Data Quality Management")
st.write("""
Welcome to the DQ Tool. This is a standalone data quality system that:
- Discovers rules from SQL files (external reference to CSB2DATA)
- Executes rules against your database
- Tracks violations and execution history

**Start by going to ⚙️ Configuration page to:**
1. Set the external SQL root path (CSB2DATA)
2. Configure database connection (DQ schema)
3. Adjust execution settings (workers & pool size)

Then navigate to other pages for Rule Definition, Management, Execution, and Auto Discovery.
""")

st.sidebar.success("Select a page to get started.")


