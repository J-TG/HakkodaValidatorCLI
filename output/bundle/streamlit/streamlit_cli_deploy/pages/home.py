"""
Home page - Migration Copilot Overview

This is the default landing page showing:
- Quick status overview
- Test data setup
- SQL query runner for exploration
"""

from datetime import datetime

import pandas as pd
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
    
    st.markdown("---")
    
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
    
    st.session_state.setdefault("test_table_status", {})
    st.session_state.setdefault("test_table_activity", {})
    st.session_state.setdefault("test_table_last_checked", None)
    st.session_state.setdefault("test_table_status_queries", [])

    def _format_status(entry):
        if not entry:
            return "—"
        exists, _, _ = entry
        return "✅ Ready" if exists else "❌ Missing"

    def _format_rows(entry):
        if entry and entry[0]:
            return entry[1]
        return None

    def _format_max_date(entry):
        if entry and entry[0] and entry[2]:
            return entry[2]
        return "—"

    def _format_activity(activity):
        return activity or "—"
    
    col_actions = st.columns([1, 1])
    with col_actions[0]:
        if st.button("🚀 Setup Test Data", type="primary", width="stretch"):
            with st.spinner("Creating and populating test tables..."):
                results = setup_test_tables(conn, runtime_mode, schema_prefix)
                activity_map = st.session_state["test_table_activity"]
                all_ok = True
                for table_name, success, message in results:
                    base_table = table_name.split(" ")[0]
                    phase = "DDL" if "(DDL)" in table_name else "DML" if "(DML)" in table_name else "Setup"
                    prefix = "✅" if success else "❌"
                    detail = message or "OK"
                    activity_map[base_table] = f"{prefix} {phase}: {detail}"
                    if not success:
                        all_ok = False
                st.session_state["test_table_activity"] = activity_map
                st.session_state["test_table_status"] = {}
                st.session_state["test_table_last_checked"] = None
                st.session_state["test_table_status_queries"] = []
                
                if all_ok:
                    st.balloons()
                    st.toast("Test data setup complete!", icon="🎉")
                else:
                    st.toast("Some tables failed — see Last Action column for details.", icon="⚠️")
                st.toast("Status cleared. Run Check Table Status to refresh.", icon="ℹ️")
                # Clear query cache to show fresh data
                run_query.clear()
    with col_actions[1]:
        if st.button("🔍 Check Table Status", width="stretch"):
            with st.spinner("Checking tables..."):
                query_log = []
                status = check_test_tables_exist(
                    conn,
                    runtime_mode,
                    schema_prefix,
                    query_log=query_log,
                )
                st.session_state["test_table_status"] = {
                    table_name: (exists, count, max_date)
                    for table_name, exists, count, max_date in status
                }
                st.session_state["test_table_last_checked"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                st.session_state["test_table_status_queries"] = query_log
                st.toast("Table statuses refreshed", icon="🔄")

    last_checked = st.session_state.get("test_table_last_checked")
    status_queries = st.session_state.get("test_table_status_queries") or []
    if last_checked:
        st.caption(f"Last checked: {last_checked}")
    else:
        st.caption("Last checked: —")
    if status_queries:
        st.caption("Executed queries during last status check")
        st.code("\n".join(status_queries), language="sql")

    status_map = st.session_state.get("test_table_status", {})
    activity_map = st.session_state.get("test_table_activity", {})

    table_catalog = pd.DataFrame(
        [
            {
                "Table": table_def["name"],
                "Fully Qualified": f"{schema_prefix}.{table_def['name']}" if schema_prefix else table_def["name"],
                "Description": table_def["description"],
                "Date Column": table_def.get("date_column", "—") or "—",
                "Status": _format_status(status_map.get(table_def["name"])),
                "Rows": _format_rows(status_map.get(table_def["name"])),
                "Max Date": _format_max_date(status_map.get(table_def["name"])),
                "Last Action": _format_activity(activity_map.get(table_def["name"]))
            }
            for table_def in TEST_TABLES
        ]
    )
    st.caption("📋 Available Test Tables")
    st.dataframe(
        table_catalog,
        width="stretch",
        hide_index=True,
        column_config={
            "Table": st.column_config.TextColumn(width="small"),
            "Description": st.column_config.TextColumn(width="medium"),
            "Date Column": st.column_config.TextColumn(width="small"),
            "Status": st.column_config.TextColumn(width="small"),
            "Rows": st.column_config.NumberColumn(width="small"),
            "Max Date": st.column_config.TextColumn(width="medium"),
            "Last Action": st.column_config.TextColumn(width="large"),
        },
    )
    
    st.markdown("---")
    
    # Sample Data Preview for each table (top 10 rows)
    st.subheader("📊 Sample Data Preview")

    table_names = [tbl["name"] for tbl in TEST_TABLES]
    prefix = get_snowflake_table_prefix() if runtime_mode != "duckdb" else None

    for table_name in table_names:
        if runtime_mode == "duckdb":
            query = f"SELECT * FROM {table_name} LIMIT 10"
            caption = f"🦆 DuckDB: {table_name}"
        else:
            table_ref = f"{prefix}.{table_name}"
            query = f"SELECT * FROM {table_ref} LIMIT 10"
            caption = f"❄️ Snowflake: {table_ref}"

        st.caption(caption)
        try:
            df_preview = run_query(conn, query, runtime_mode)
            st.dataframe(df_preview, width="stretch")
        except Exception as e:
            st.warning(f"Unable to preview {table_name}: {e}")


# Run the page
main()
