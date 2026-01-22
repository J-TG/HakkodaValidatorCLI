"""
Settings page

Application settings and configuration.

Features:
- Database connection settings
- Test data configuration
- User preferences
- System information
"""

from copy import deepcopy

import streamlit as st
from common.layout import init_page, get_runtime_mode, get_db_connection
from common.db import (
    get_snowflake_table_prefix,
    DEFAULT_SNOWFLAKE_DATABASE,
    DEFAULT_SNOWFLAKE_SCHEMA,
    DEFAULT_INGESTION_METADATA_CONFIG,
    METADATA_CONFIG_TABLE_NAME,
    METADATA_ENVIRONMENTS,
    get_ingestion_metadata_env,
    get_ingestion_metadata_table_path,
)


def main():
    """Main function for Settings page."""
    
    # Initialize page
    conn = init_page()
    runtime_mode = get_runtime_mode()
    
    # Page header
    st.title("⚙️ Settings")
    st.markdown("Application settings and configuration.")
    
    # Settings tabs
    tab_conn, tab_test, tab_ingestion, tab_system = st.tabs(
        ["🔌 Connection", "📊 Test Data", "🧪 Ingestion", "ℹ️ System Info"]
    )
    
    with tab_conn:
        st.subheader("Database Connection")
        
        st.markdown(f"**Current Runtime Mode:** `{runtime_mode}`")
        
        if runtime_mode == "duckdb":
            st.info("🦆 Running with local DuckDB (in-memory)")
            st.markdown("""
            DuckDB is being used for local development. Data is stored in-memory 
            and will be lost when the app restarts.
            
            To connect to Snowflake instead, run:
            ```bash
            make run-snowflake
            ```
            """)
        
        elif runtime_mode == "snowflake_local":
            st.info("❄️ Running locally with Snowflake connector")
            st.markdown("""
            Connected to Snowflake using credentials from `.streamlit/secrets.toml`.
            
            This mode uses the `snowflake-connector-python` package for database access.
            """)
        
        elif runtime_mode == "snowflake_deployed":
            st.success("☁️ Running in Snowflake (deployed)")
            st.markdown("""
            Running inside Snowflake's managed Streamlit environment.
            
            Using Snowpark session for optimal performance.
            """)
        
        st.markdown("---")
        
        st.markdown("### Connection Status")
        if conn:
            st.success("✅ Database connection active")
        else:
            st.error("❌ No database connection")
    
    with tab_test:
        st.subheader("Test Data Configuration")
        
        if runtime_mode in ("snowflake_local", "snowflake_deployed"):
            st.markdown("Configure the target location for test data tables.")
            
            col1, col2 = st.columns(2)
            
            with col1:
                new_database = st.text_input(
                    "Database",
                    value=st.session_state.get("test_data_database", DEFAULT_SNOWFLAKE_DATABASE),
                    help="Snowflake database for test tables",
                )
            
            with col2:
                new_schema = st.text_input(
                    "Schema",
                    value=st.session_state.get("test_data_schema", DEFAULT_SNOWFLAKE_SCHEMA),
                    help="Snowflake schema for test tables",
                )
            
            if st.button("Save Configuration"):
                st.session_state["test_data_database"] = new_database
                st.session_state["test_data_schema"] = new_schema
                st.success(f"Configuration saved: `{new_database}.{new_schema}`")
            
            st.markdown("---")
            st.markdown(f"**Current Target:** `{get_snowflake_table_prefix()}`")
        
        else:
            st.info("🦆 DuckDB mode - test data is stored in-memory")
            st.markdown("""
            In DuckDB mode, all test data is created in-memory and 
            persists only for the duration of the app session.
            """)
    
    with tab_ingestion:
        st.subheader("Ingestion Metadata")
        st.markdown(
            "Manage the database/schema used for the METADATA_CONFIG_TABLE_ELT table "
            "across environments. These settings drive Copilot metrics and query previews."
        )

        env_options = list(METADATA_ENVIRONMENTS)
        current_env = get_ingestion_metadata_env()
        selected_env = st.selectbox(
            "Active Environment",
            env_options,
            index=env_options.index(current_env),
        )
        if selected_env != current_env:
            st.session_state["ingestion_metadata_env"] = selected_env
            st.success(f"Active ingestion environment set to `{selected_env}`")
            current_env = selected_env

        st.info(
            f"Current table: `{get_ingestion_metadata_table_path(current_env)}`"
        )
        st.caption(
            "All Copilot metrics and saved queries reference the active environment above."
        )

        st.markdown("---")

        configs = st.session_state.get(
            "ingestion_metadata_config",
            deepcopy(DEFAULT_INGESTION_METADATA_CONFIG),
        )

        for env in env_options:
            env_defaults = DEFAULT_INGESTION_METADATA_CONFIG.get(
                env, {"database": f"STAGE_{env}", "schema": "ELT"}
            )
            env_config = configs.get(env, env_defaults)
            with st.expander(
                f"{env} configuration", expanded=(env == current_env)
            ):
                with st.form(f"ingestion-config-{env}"):
                    db_value = st.text_input(
                        f"{env} Database",
                        value=env_config.get("database", env_defaults["database"]),
                    )
                    schema_value = st.text_input(
                        f"{env} Schema",
                        value=env_config.get("schema", env_defaults["schema"]),
                    )
                    st.caption(
                        f"Preview: `{db_value or env_defaults['database']}."
                        f"{schema_value or env_defaults['schema']}."
                        f"{METADATA_CONFIG_TABLE_NAME}`"
                    )
                    submitted = st.form_submit_button("Save")
                    if submitted:
                        updated_configs = deepcopy(configs)
                        updated_configs[env] = {
                            "database": (db_value or env_defaults["database"]).strip(),
                            "schema": (schema_value or env_defaults["schema"]).strip(),
                        }
                        st.session_state["ingestion_metadata_config"] = updated_configs
                        configs = updated_configs
                        st.success(f"{env} metadata location updated")

    with tab_system:
        st.subheader("System Information")
        
        # Runtime info
        st.markdown("### Runtime Environment")
        
        info_data = {
            "Runtime Mode": runtime_mode,
            "Connection Type": type(conn).__name__ if conn else "None",
        }
        
        for key, value in info_data.items():
            st.markdown(f"**{key}:** `{value}`")
        
        st.markdown("---")
        
        # Session state debug
        with st.expander("🔧 Session State (Debug)", expanded=False):
            st.json({k: str(v) for k, v in st.session_state.items()})
        
        st.markdown("---")
        
        st.markdown("### App Information")
        st.markdown("""
        **Migration Copilot** v0.1.0
        
        A Streamlit app for monitoring and managing data migration pipelines.
        
        - Built with Streamlit
        - Supports DuckDB (local) and Snowflake
        - Deployable via Snowflake CLI
        """)


# Run the page
main()
