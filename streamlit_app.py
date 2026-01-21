import os
from pathlib import Path
import streamlit as st
from common.hello import say_hello
from common.db import (
    detect_runtime,
    get_connection,
    run_query,
    get_testtable_query,
    get_snowflake_table_prefix,
    setup_test_tables,
    check_test_tables_exist,
    DEFAULT_SNOWFLAKE_DATABASE,
    DEFAULT_SNOWFLAKE_SCHEMA,
)
from common.test_data import get_table_names, TEST_TABLES

import pandas as pd


def load_env_file(env_path: Path) -> None:
    """Load environment variables from a file (does not override existing)."""
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


# Load local env files for prefills (does not override existing env)
load_env_file(Path(".env.local"))
load_env_file(Path(".env.snowflake-dev"))
load_env_file(Path(".env.duckdb"))

# ============================================================================
# Auto-detect runtime mode (no manual selection needed)
# ============================================================================
runtime_mode = detect_runtime()

# Store in session state for consistency
if "runtime_mode" not in st.session_state:
    st.session_state["runtime_mode"] = runtime_mode

# Initialize test data config in session state
if "test_data_database" not in st.session_state:
    st.session_state["test_data_database"] = DEFAULT_SNOWFLAKE_DATABASE
if "test_data_schema" not in st.session_state:
    st.session_state["test_data_schema"] = DEFAULT_SNOWFLAKE_SCHEMA

# ============================================================================
# Page setup
# ============================================================================
st.title(f"Example streamlit app. {say_hello()}")

# Show runtime status in sidebar
st.sidebar.header("Runtime Status")
if runtime_mode == "duckdb":
    st.sidebar.success("🦆 DuckDB (local, in-memory)")
elif runtime_mode == "snowflake_local":
    st.sidebar.success("❄️ Snowflake (local connector)")
elif runtime_mode == "snowflake_deployed":
    st.sidebar.success("☁️ Snowflake (deployed)")

st.sidebar.caption(f"Mode: `{runtime_mode}`")

# ============================================================================
# Database connection
# ============================================================================
try:
    conn = get_connection(runtime_mode)
    st.session_state["db_conn"] = conn
except Exception as e:
    st.error(f"Failed to connect to database: {e}")
    st.stop()

# ============================================================================
# Test Data Setup Section (Sidebar)
# ============================================================================
st.sidebar.markdown("---")
st.sidebar.header("🧪 Test Data Setup")

# For Snowflake modes, allow configurable database/schema
if runtime_mode in ("snowflake_local", "snowflake_deployed"):
    st.session_state["test_data_database"] = st.sidebar.text_input(
        "Target Database",
        value=st.session_state["test_data_database"],
        help="Snowflake database for test tables",
    )
    st.session_state["test_data_schema"] = st.sidebar.text_input(
        "Target Schema",
        value=st.session_state["test_data_schema"],
        help="Snowflake schema for test tables",
    )
    schema_prefix = get_snowflake_table_prefix()
    st.sidebar.caption(f"Tables will be created in: `{schema_prefix}`")
else:
    schema_prefix = ""
    st.sidebar.caption("Tables will be created in DuckDB (in-memory)")

# Show available tables
with st.sidebar.expander("📋 Available Test Tables", expanded=False):
    for table_def in TEST_TABLES:
        st.markdown(f"**{table_def['name']}**: {table_def['description']}")

# Check current table status
if st.sidebar.button("🔍 Check Table Status"):
    with st.spinner("Checking tables..."):
        status = check_test_tables_exist(conn, runtime_mode, schema_prefix)
        for table_name, exists, count in status:
            if exists:
                st.sidebar.success(f"✅ {table_name}: {count} rows")
            else:
                st.sidebar.warning(f"❌ {table_name}: not found")

# Setup test data button
if st.sidebar.button("🚀 Setup Test Data", type="primary"):
    with st.spinner("Creating and populating test tables..."):
        results = setup_test_tables(conn, runtime_mode, schema_prefix)
        
        all_ok = True
        for table_name, success, message in results:
            if success:
                st.sidebar.success(f"✅ {table_name}: {message}")
            else:
                st.sidebar.error(f"❌ {table_name}: {message}")
                all_ok = False
        
        if all_ok:
            st.sidebar.balloons()
            st.toast("Test data setup complete!", icon="🎉")
            # Clear query cache to show fresh data
            run_query.clear()

st.sidebar.markdown("---")

# ============================================================================
# Main: query input and execution
# ============================================================================
if runtime_mode == "duckdb":
    default_query = "SELECT * FROM SAMPLE_DATA LIMIT 50"
else:
    prefix = get_snowflake_table_prefix()
    default_query = f"SELECT * FROM {prefix}.TESTTABLE LIMIT 10"

query = st.text_area("SQL query", value=default_query, height=200)

if st.button("Run Query"):
    try:
        df = run_query(conn, query, runtime_mode)
        st.dataframe(df)
    except Exception as e:
        st.error(f"Query failed: {e}")

# ============================================================================
# TESTTABLE preview section
# ============================================================================
st.markdown("---")
st.subheader("TESTTABLE Preview")

try:
    testtable_query = get_testtable_query(runtime_mode)
    test_df = run_query(conn, testtable_query, runtime_mode)

    if runtime_mode == "duckdb":
        st.caption("🦆 DuckDB: TESTTABLE (sample data)")
    else:
        prefix = get_snowflake_table_prefix()
        st.caption(f"❄️ Snowflake: {prefix}.TESTTABLE")

    st.dataframe(test_df)

    # Show chart if data available
    if not test_df.empty and "CC_EMPLOYEES" in test_df.columns:
        chart_df = test_df[["CC_CALL_CENTER_ID", "CC_EMPLOYEES"]].dropna()
        if not chart_df.empty:
            st.bar_chart(chart_df, x="CC_CALL_CENTER_ID", y="CC_EMPLOYEES")

except Exception as e:
    if runtime_mode == "duckdb":
        st.info("🦆 No test data yet. Click **Setup Test Data** in the sidebar to create tables.")
    else:
        # In Snowflake mode, if table missing, don't show sample data per user request
        st.warning(f"Snowflake TESTTABLE not available. Click **Setup Test Data** in the sidebar to create it.\n\nError: {e}")

# ============================================================================
# Footer
# ============================================================================
st.markdown("---")
st.write("Runtime mode:", runtime_mode)

