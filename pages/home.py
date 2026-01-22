"""
Home page - Migration Copilot Overview

This is the default landing page showing:
- Quick status overview
- Test data setup
- SQL query runner for exploration
"""

import streamlit as st
from common.layout import init_page, get_runtime_mode, get_db_connection
from common.db import (
    run_query,
    get_testtable_query,
    get_snowflake_table_prefix,
    setup_test_tables,
    check_test_tables_exist,
    DEFAULT_SNOWFLAKE_DATABASE,
    DEFAULT_SNOWFLAKE_SCHEMA,
)
from common.test_data import TEST_TABLES


def main():
    """Main function for the Home page."""
    
    # Initialize page and get connection
    conn = init_page()
    runtime_mode = get_runtime_mode()
    
    # Page header
    st.title("🚀 Migration Copilot")
    st.markdown("Welcome to the Migration Copilot - your data pipeline monitoring and management hub.")
    
    # Quick status cards
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric(
            label="Runtime Mode",
            value=runtime_mode.replace("_", " ").title(),
        )
    
    with col2:
        st.metric(
            label="Connection",
            value="✅ Connected" if conn else "❌ Disconnected",
        )
    
    with col3:
        # Show schema context for Snowflake
        if runtime_mode in ("snowflake_local", "snowflake_deployed"):
            schema_prefix = get_snowflake_table_prefix()
            st.metric(label="Target Schema", value=schema_prefix)
        else:
            st.metric(label="Database", value="DuckDB (in-memory)")
    
    st.markdown("---")
    
    # Two-column layout
    left_col, right_col = st.columns([2, 1])
    
    with left_col:
        # SQL Query Runner
        st.subheader("🔍 SQL Query Runner")
        
        if runtime_mode == "duckdb":
            default_query = "SELECT * FROM SAMPLE_DATA LIMIT 50;"
        else:
            prefix = get_snowflake_table_prefix()
            default_query = f"SELECT * FROM {prefix}.TESTTABLE LIMIT 10;"
        
        query = st.text_area("SQL query", value=default_query, height=150)
        
        if st.button("Run Query", type="primary"):
            try:
                df = run_query(conn, query, runtime_mode)
                st.dataframe(df, width="stretch")
            except Exception as e:
                st.error(f"Query failed: {e}")
        
        # TESTTABLE Preview
        st.markdown("---")
        st.subheader("📊 Sample Data Preview")
        
        try:
            testtable_query = get_testtable_query(runtime_mode)
            test_df = run_query(conn, testtable_query, runtime_mode)
            
            if runtime_mode == "duckdb":
                st.caption("🦆 DuckDB: TESTTABLE")
            else:
                prefix = get_snowflake_table_prefix()
                st.caption(f"❄️ Snowflake: {prefix}.TESTTABLE")
            
            st.dataframe(test_df, width="stretch")
            
            # Show chart if data available
            if not test_df.empty and "CC_EMPLOYEES" in test_df.columns:
                chart_df = test_df[["CC_CALL_CENTER_ID", "CC_EMPLOYEES"]].dropna()
                if not chart_df.empty:
                    st.bar_chart(chart_df, x="CC_CALL_CENTER_ID", y="CC_EMPLOYEES")
        
        except Exception as e:
            if runtime_mode == "duckdb":
                st.info("🦆 No test data yet. Use the **Test Data Setup** panel to create tables.")
            else:
                st.warning(f"Snowflake TESTTABLE not available. Use **Test Data Setup** to create it.\n\nError: {e}")
    
    with right_col:
        # Test Data Setup Panel
        st.subheader("🧪 Test Data Setup")
        
        # For Snowflake modes, allow configurable database/schema
        if runtime_mode in ("snowflake_local", "snowflake_deployed"):
            st.session_state["test_data_database"] = st.text_input(
                "Target Database",
                value=st.session_state.get("test_data_database", DEFAULT_SNOWFLAKE_DATABASE),
                help="Snowflake database for test tables",
            )
            st.session_state["test_data_schema"] = st.text_input(
                "Target Schema",
                value=st.session_state.get("test_data_schema", DEFAULT_SNOWFLAKE_SCHEMA),
                help="Snowflake schema for test tables",
            )
            schema_prefix = get_snowflake_table_prefix()
            st.caption(f"Tables will be created in: `{schema_prefix}`")
        else:
            schema_prefix = ""
            st.caption("Tables will be created in DuckDB (in-memory)")
        
        # Show available tables
        with st.expander("📋 Available Test Tables", expanded=False):
            for table_def in TEST_TABLES:
                st.markdown(f"**{table_def['name']}**: {table_def['description']}")
        
        # Check current table status
        if st.button("🔍 Check Table Status"):
            with st.spinner("Checking tables..."):
                status = check_test_tables_exist(conn, runtime_mode, schema_prefix)
                for table_name, exists, count, max_date in status:
                    if exists:
                        date_info = f", max date: {max_date}" if max_date else ""
                        st.success(f"✅ {table_name}: {count} rows{date_info}")
                    else:
                        st.warning(f"❌ {table_name}: not found")
        
        # Setup test data button
        if st.button("🚀 Setup Test Data", type="primary", width="stretch"):
            with st.spinner("Creating and populating test tables..."):
                results = setup_test_tables(conn, runtime_mode, schema_prefix)
                
                all_ok = True
                for table_name, success, message in results:
                    if success:
                        st.success(f"✅ {table_name}: {message}")
                    else:
                        st.error(f"❌ {table_name}: {message}")
                        all_ok = False
                
                if all_ok:
                    st.balloons()
                    st.toast("Test data setup complete!", icon="🎉")
                    # Clear query cache to show fresh data
                    run_query.clear()


# Run the page
main()
