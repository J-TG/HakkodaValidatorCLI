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
    run_query,
    execute_ddl_dml,
)


TEAM_SECTIONS = {
    "Ingestion Team": "INGESTION",
    "Modeling Team": "MODELING",
    "BI Team": "BI",
}


def main():
    """Main function for Settings page."""
    
    # Initialize page
    conn = init_page()
    runtime_mode = get_runtime_mode()
    
    # Page header
    st.title("⚙️ Settings")
    st.markdown("Application settings and configuration.")
    
    # Settings tabs
    tab_conn, tab_test, tab_team, tab_validation, tab_ingestion, tab_system = st.tabs(
        [
            "🔌 Connection",
            "📊 Test Data",
            "👥 Team Members",
            "✅ Validation",
            "🧪 Ingestion",
            "ℹ️ System Info",
        ]
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

    with tab_team:
        st.subheader("Team Members")
        st.markdown(
            "Maintain functional rosters so other pages can reference who owns each pipeline segment."
        )

        if runtime_mode in ("snowflake_local", "snowflake_deployed"):
            team_table = f"{get_snowflake_table_prefix()}.TEAM_MEMBERS"
        else:
            team_table = "TEAM_MEMBERS"

        st.caption(f"Roster records live in `{team_table}`")

        st.markdown("#### Add a teammate")
        with st.form("team_member_form"):
            team_choices = list(TEAM_SECTIONS.keys())
            team_label = st.selectbox("Team *", team_choices, index=0)
            full_name = st.text_input("Full Name *", placeholder="e.g., Maya Rivera")
            username = st.text_input(
                "Username (email) *",
                placeholder="name@example.com",
            )
            responsibility = st.text_input(
                "Responsibility / Focus",
                placeholder="Pipelines, quality gates, dashboards, ...",
            )
            add_member = st.form_submit_button("➕ Add Team Member", type="primary")

        if add_member:
            if not full_name.strip() or not username.strip():
                st.error("Full name and username are required.")
            elif "@" not in username or "." not in username.split("@")[-1]:
                st.error("Enter a valid email address for the username.")
            else:
                def _escape(value: str) -> str:
                    return value.replace("'", "''")

                team_value = TEAM_SECTIONS.get(team_label, "INGESTION")
                resp_sql = f"'{_escape(responsibility)}'" if responsibility else "NULL"
                insert_sql = (
                    f"INSERT INTO {team_table} (TEAM, MEMBER_NAME, USERNAME, RESPONSIBILITY) "
                    f"VALUES ('{team_value}', '{_escape(full_name.strip())}', '{_escape(username.lower().strip())}', {resp_sql});"
                )
                ok, msg = execute_ddl_dml(conn, runtime_mode, insert_sql)
                if ok:
                    run_query.clear()
                    st.success(f"Added {full_name.strip()} to the {team_label} roster.")
                else:
                    st.error(f"Unable to add team member: {msg}")

        st.markdown("---")
        st.markdown("### Current roster")
        try:
            roster_df = run_query(
                conn,
                f"SELECT TEAM, MEMBER_NAME, USERNAME, RESPONSIBILITY, CREATED_AT FROM {team_table} ORDER BY TEAM, MEMBER_NAME;",
                runtime_mode,
            )
            if roster_df.empty:
                st.info("No team members recorded yet. Use the form above to add the first teammate.")
            else:
                st.dataframe(
                    roster_df,
                    width="stretch",
                    hide_index=True,
                    column_config={
                        "TEAM": st.column_config.TextColumn("Team", width="small"),
                        "MEMBER_NAME": st.column_config.TextColumn("Member", width="medium"),
                        "USERNAME": st.column_config.TextColumn("Username", width="medium"),
                        "RESPONSIBILITY": st.column_config.TextColumn("Responsibility", width="large"),
                        "CREATED_AT": st.column_config.TextColumn("Added", width="small"),
                    },
                )

                st.markdown("#### Team breakdown")
                for label, code in TEAM_SECTIONS.items():
                    section_df = roster_df[roster_df["TEAM"] == code]
                    with st.expander(f"{label} ({len(section_df)})", expanded=True):
                        if section_df.empty:
                            st.caption("No members recorded yet.")
                        else:
                            display_df = section_df[
                                ["MEMBER_NAME", "USERNAME", "RESPONSIBILITY"]
                            ].rename(
                                columns={
                                    "MEMBER_NAME": "Member",
                                    "USERNAME": "Username",
                                    "RESPONSIBILITY": "Responsibility",
                                }
                            )
                            st.dataframe(display_df, width="stretch", hide_index=True)
        except Exception as exc:
            st.warning(
                "Unable to retrieve team members. Ensure the TEAM_MEMBERS table exists via Test Data Setup."
            )
            st.exception(exc)

    with tab_validation:
        st.subheader("Validation Objects Registry")
        st.markdown(
            "Capture validation targets directly in Snowflake for downstream monitoring and compatibility checks."
        )

        if runtime_mode == "duckdb":
            st.warning(
                "Validation objects persist only for the current DuckDB session. Switch to Snowflake for permanent storage."
            )

        if runtime_mode in ("snowflake_local", "snowflake_deployed"):
            target_table = f"{get_snowflake_table_prefix()}.VALIDATION_OBJECTS"
        else:
            target_table = "VALIDATION_OBJECTS"

        st.caption(f"Records are stored in `{target_table}`")

        with st.form("validation_objects_form"):
            name = st.text_input("Name *", placeholder="e.g., Finance compatibility view")
            fully_qualified_name = st.text_input(
                "Fully-Qualified Name *",
                placeholder="DATABASE.SCHEMA.OBJECT",
            )
            description = st.text_area("Description", height=80)
            submitted = st.form_submit_button("Add Validation Object", type="primary")

        if submitted:
            if not name or not fully_qualified_name:
                st.error("Name and Fully-Qualified Name are required.")
            else:
                def _escape(value: str) -> str:
                    return value.replace("'", "''")

                desc_value = f"'{_escape(description)}'" if description else "NULL"
                insert_sql = (
                    f"INSERT INTO {target_table} (NAME, FULLY_QUALIFIED_NAME, DESCRIPTION) "
                    f"VALUES ('{_escape(name)}', '{_escape(fully_qualified_name)}', {desc_value});"
                )
                ok, msg = execute_ddl_dml(conn, runtime_mode, insert_sql)
                if ok:
                    run_query.clear()
                    st.success(f"Validation object '{name}' added.")
                else:
                    st.error(f"Unable to add validation object: {msg}")

        st.markdown("---")
        st.markdown("### Current Validation Objects")
        try:
            df = run_query(
                conn,
                f"SELECT NAME, FULLY_QUALIFIED_NAME, DESCRIPTION, CREATED_AT FROM {target_table} ORDER BY CREATED_AT DESC;",
                runtime_mode,
            )
            if df.empty:
                st.info("No validation objects found yet.")
            else:
                st.dataframe(df, width="stretch", hide_index=True)
        except Exception as exc:
            st.warning(f"Unable to read validation objects: {exc}")
    
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
