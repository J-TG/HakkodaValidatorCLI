"""Ingestion Copilot page with build/run workflow."""

from __future__ import annotations

from typing import Any, Dict, List, Tuple
from datetime import datetime, date, time, timedelta
import uuid

import streamlit as st

from common.layout import init_page, get_runtime_mode
from common.db import execute_ddl_dml, get_snowflake_table_prefix, run_query, get_ingestion_metadata_table
from common.ingestion_templates import (
    ADHOC_ENVIRONMENTS,
    ADF_POLL_SCHEDULE,
    INGESTION_ENVIRONMENTS,
    build_adhoc_insert_sql,
    build_schedule_update_sql,
    build_schedule_sets_and_call,
)


StatementList = List[Tuple[str, str]]

SQL_SERVER_DEFAULTS: Dict[str, Any] = {
    "logical_name": "AgentCubedODS",
    "database_name": "AgentCubedODS",
    "schema_name": "A3_CURR",
    "source_table_name": "AgencyManagementParty",
    "file_landing_schema": "",
    "file_wildcard_pattern": "",
    "file_table_name": "",
    "file_extension": "",
    "sheet_name": "",
    "file_format": "HEART_PARQUET_FORMAT",
    "has_header": "Y",
    "delta_column": "LOAD_DATETIME",
    "change_tracking_type": "INC",
    "priority_flag": False,
    "server_name": "SHAREDSQL-TEST",
    "file_server_load_type": "",
    "filestamp_format": "",
    "auto_ingest": "",
    "file_server_filepath": "",
    "fixed_width_filetype": "",
    "regex_pattern": "",
}

SQL_SERVER_DELTA_COLUMNS = [
    "LOAD_DATETIME",
    "SysUpdateDateTime",
    "SysInsertDateTime",
]
SQL_SERVER_CHANGE_TRACKING = ["INC", "FULL"]
SQL_SERVER_DATABASE_EXAMPLES = [
    "AgentCubedODS",
    "MindfulODS",
    "GenesysODS",
    "MDS",
    "BIDProcess",
    "ElectronicEnrollment",
]
SQL_SERVER_SERVER_EXAMPLES = [
    "SHAREDSQL-TEST",
    "ScanODS-Test",
    "BIMILL-APP-TEST",
    "EDI-Test01",
    "BI-RPT-TEST",
]
SOURCE_TYPE_PANELS = [
    {
        "label": "SQL Server",
        "description": "Metadata-backed loads from METADATA_CONFIG_TABLE_ELT.",
        "enabled": True,
    },
    {
        "label": "Flat File",
        "description": "File containers and wildcard rules.",
        "enabled": False,
    },
    {
        "label": "Azure DB",
        "description": "Managed connectors for Azure SQL.",
        "enabled": False,
    },
    {
        "label": "Excel File",
        "description": "Sheet-driven ingestion playbooks.",
        "enabled": False,
    },
]
SOURCE_TYPE_CARD_STYLES = """
<style>
.source-type-grid {display: flex; flex-wrap: wrap; gap: 0.75rem; margin-bottom: 0.5rem;}
.source-type-card {flex: 1 1 180px; border: 1px solid #3a3a3a; border-radius: 8px; padding: 0.75rem; text-align: center; background-color: rgba(58,58,58,0.05);}
.source-type-card .label {font-weight: 600; margin-top: 0.15rem;}
.source-type-card .status {font-size: 0.8rem; text-transform: uppercase; letter-spacing: 0.08em; opacity: 0.7;}
.source-type-card.active {border-color: #3aa9ff; background-color: rgba(58,169,255,0.08);}
.source-type-card.disabled {opacity: 0.35; border-style: dashed;}
</style>
"""


def _get_requester() -> str:
    return st.session_state.get("user_email", "streamlit_user")


def _table_exists(conn: Any, table_name: str, runtime_mode: str = "duckdb") -> bool:
    """Check if a table or view exists by attempting to query it."""
    try:
        run_query(conn, f"SELECT 1 FROM {table_name} LIMIT 1", runtime_mode)
        return True
    except Exception:
        return False


def _render_source_type_palette(active_label: str) -> None:
    st.markdown(SOURCE_TYPE_CARD_STYLES, unsafe_allow_html=True)
    cards = ["<div class='source-type-grid'>"]
    for panel in SOURCE_TYPE_PANELS:
        classes = ["source-type-card"]
        if panel["label"] == active_label and panel["enabled"]:
            classes.append("active")
        if not panel["enabled"]:
            classes.append("disabled")
        status = "Active" if panel["enabled"] else "Coming Soon"
        cards.append(
            "".join(
                [
                    f"<div class='{' '.join(classes)}'>",
                    f"<div class='status'>{status}</div>",
                    f"<div class='label'>{panel['label']}</div>",
                    f"<p>{panel['description']}</p>",
                    "</div>",
                ]
            )
        )
    cards.append("</div>")
    st.markdown("".join(cards), unsafe_allow_html=True)


def _load_sql_source_options(conn: Any, runtime_mode: str) -> List[Dict[str, Any]]:
    if conn is None:
        return []

    view_name = "VW_METADATA_CONFIG_TABLE_ELT_MASTER"
    prefix = "" if runtime_mode == "duckdb" else get_snowflake_table_prefix()
    table_ref = f"{prefix}.{view_name}" if prefix else view_name
    query = (
        "SELECT DISTINCT "
        "LOGICAL_NAME, DATABASE_NAME, SCHEMA_NAME, SERVER_NAME, "
        "COALESCE(ENVIRONMENT, '') || ' | ' || COALESCE(LOGICAL_NAME, '') || ' | ' || "
        "COALESCE(DATABASE_NAME, '') || '.' || COALESCE(SCHEMA_NAME, '') || ' | ' || "
        "COALESCE(SERVER_NAME, '') AS SOURCE_LABEL "
        f"FROM {table_ref} ORDER BY SOURCE_LABEL"
    )

    try:
        df = run_query(conn, query, runtime_mode)
    except Exception:
        st.info("SQL metadata view not available yet. Defaults stay pre-populated.")
        return []

    if df is None or df.empty:
        return []
    return df.to_dict("records")


def _load_delta_columns(
    conn: Any,
    runtime_mode: str,
    logical_name: str,
    database_name: str,
    schema_name: str,
    server_name: str,
    source_table_name: str = "",
) -> List[str]:
    if conn is None:
        return []

    view_name = "VW_METADATA_CONFIG_TABLE_ELT_MASTER"
    prefix = "" if runtime_mode == "duckdb" else get_snowflake_table_prefix()
    table_ref = f"{prefix}.{view_name}" if prefix else view_name
    query = (
        "SELECT DISTINCT DELTA_COLUMN "
        f"FROM {table_ref} "
        f"WHERE LOGICAL_NAME = '{_sql_literal(logical_name)}' "
        f"AND DATABASE_NAME = '{_sql_literal(database_name)}' "
        f"AND SCHEMA_NAME = '{_sql_literal(schema_name)}' "
        f"AND COALESCE(SERVER_NAME, '') = COALESCE('{_sql_literal(server_name)}', '') "
        + (f"AND SOURCE_TABLE_NAME = '{_sql_literal(source_table_name)}' " if source_table_name else "")
        + "AND DELTA_COLUMN IS NOT NULL "
        + "ORDER BY DELTA_COLUMN"
    )

    try:
        df = run_query(conn, query, runtime_mode)
    except Exception:
        return []

    if df is None or df.empty:
        return []
    return sorted({str(v) for v in df["DELTA_COLUMN"].dropna().tolist()})


def _load_change_tracking(
    conn: Any,
    runtime_mode: str,
    logical_name: str,
    database_name: str,
    schema_name: str,
    server_name: str,
    source_table_name: str = "",
) -> List[str]:
    if conn is None:
        return []

    view_name = "VW_METADATA_CONFIG_TABLE_ELT_MASTER"
    prefix = "" if runtime_mode == "duckdb" else get_snowflake_table_prefix()
    table_ref = f"{prefix}.{view_name}" if prefix else view_name
    query = (
        "SELECT DISTINCT CHANGE_TRACKING_TYPE "
        f"FROM {table_ref} "
        f"WHERE LOGICAL_NAME = '{_sql_literal(logical_name)}' "
        f"AND DATABASE_NAME = '{_sql_literal(database_name)}' "
        f"AND SCHEMA_NAME = '{_sql_literal(schema_name)}' "
        f"AND COALESCE(SERVER_NAME, '') = COALESCE('{_sql_literal(server_name)}', '') "
        + (f"AND SOURCE_TABLE_NAME = '{_sql_literal(source_table_name)}' " if source_table_name else "")
        + "AND CHANGE_TRACKING_TYPE IS NOT NULL "
        + "ORDER BY CHANGE_TRACKING_TYPE"
    )

    try:
        df = run_query(conn, query, runtime_mode)
    except Exception:
        return []

    if df is None or df.empty:
        return []
    return sorted({str(v) for v in df["CHANGE_TRACKING_TYPE"].dropna().tolist()})


def _sql_literal(value: str) -> str:
    return (value or "").replace("'", "''")


def _load_source_tables(
    conn: Any,
    runtime_mode: str,
    logical_name: str,
    database_name: str,
    schema_name: str,
    server_name: str,
) -> List[str]:
    if conn is None:
        return []
    if not schema_name:
        return []

    view_name = "VW_METADATA_CONFIG_TABLE_ELT_MASTER"
    prefix = "" if runtime_mode == "duckdb" else get_snowflake_table_prefix()
    table_ref = f"{prefix}.{view_name}" if prefix else view_name
    query = (
        "SELECT DISTINCT SOURCE_TABLE_NAME "
        f"FROM {table_ref} "
        f"WHERE LOGICAL_NAME ILIKE '%{_sql_literal(logical_name)}%' "
        f"AND DATABASE_NAME ILIKE '%{_sql_literal(database_name)}%' "
        f"AND SCHEMA_NAME ILIKE '%{_sql_literal(schema_name)}%' "
        f"AND COALESCE(SERVER_NAME, '') ILIKE '%{_sql_literal(server_name)}%' "
        "ORDER BY SOURCE_TABLE_NAME"
    )

    try:
        df = run_query(conn, query, runtime_mode)
    except Exception:
        return []

    if df is None or df.empty:
        return []
    return sorted({str(v) for v in df["SOURCE_TABLE_NAME"].dropna().tolist()})


def _render_ingestion_builder(conn: Any, runtime_mode: str) -> None:
    st.subheader("Configure New Source")
    st.caption(ADF_POLL_SCHEDULE)
    active_source_type = "SQL Server"
    _render_source_type_palette(active_source_type)
    st.caption("Only SQL Server metadata pipelines are enabled today. Other source types remain greyed out until their playbooks are ready.")

    sql_source_options = _load_sql_source_options(conn, runtime_mode)

    manual_picklist_label = "Manual entry"

    with st.form("ingestion_builder"):
        col_env, col_type = st.columns(2)
        environment = col_env.selectbox("Environment", INGESTION_ENVIRONMENTS, index=0)
        col_type.markdown("**Source Type**")
        col_type.success("SQL Server (active)")
        adhoc_allowed = environment in ADHOC_ENVIRONMENTS
        schedule_mode = st.selectbox(
            "Ingestion mode",
            options=["Scheduled", "Adhoc"],
            index=0 if not adhoc_allowed else 0,
            help="Scheduled runs use INSERT_METADATA_CONFIG_TABLE. Adhoc queues an on-demand trigger where supported (DEV/TEST/UAT).",
        )
        # When scheduled runs are selected, allow the user to pick a start/stop window
        schedule_start_dt = None
        schedule_stop_dt = None
        if schedule_mode == "Scheduled":
            col_start, col_stop = st.columns(2)
            today = datetime.now().date()
            default_start_date = today
            default_stop_date = today + timedelta(days=3)
            start_date = col_start.date_input("Schedule Start Date", value=default_start_date)
            stop_date = col_stop.date_input("Schedule Stop Date", value=default_stop_date)
        sql_defaults = SQL_SERVER_DEFAULTS
        picklist_options = [manual_picklist_label] + [opt["SOURCE_LABEL"] for opt in sql_source_options]
        selected_label = st.selectbox(
            "Existing SQL metadata (optional)",
            options=picklist_options,
            index=0,
            help="Pulls from METADATA_SQL_SOURCES_PICKLIST_VW when available.",
        )
        selected_source = (
            next((opt for opt in sql_source_options if opt["SOURCE_LABEL"] == selected_label), None)
            if selected_label != manual_picklist_label
            else None
        )

        debug_view_name = "VW_METADATA_CONFIG_TABLE_ELT_MASTER"
        debug_prefix = "" if runtime_mode == "duckdb" else get_snowflake_table_prefix()
        debug_table_ref = f"{debug_prefix}.{debug_view_name}" if debug_prefix else debug_view_name
        debug_sql_metadata = (
            "SELECT DISTINCT "
            "LOGICAL_NAME, DATABASE_NAME, SCHEMA_NAME, SERVER_NAME, "
            "COALESCE(ENVIRONMENT, '') || ' | ' || COALESCE(LOGICAL_NAME, '') || ' | ' || "
            "COALESCE(DATABASE_NAME, '') || '.' || COALESCE(SCHEMA_NAME, '') || ' | ' || "
            "COALESCE(SERVER_NAME, '') AS SOURCE_LABEL "
            f"FROM {debug_table_ref} ORDER BY SOURCE_LABEL"
        )

        source_table_name = st.session_state.get(
            "source_table_name_select",
            sql_defaults["source_table_name"],
        )

        if selected_source:
            logical_name_filter = selected_source.get("LOGICAL_NAME") or ""
            database_name_filter = selected_source.get("DATABASE_NAME") or ""
            schema_name_filter = selected_source.get("SCHEMA_NAME") or ""
            server_name_filter = selected_source.get("SERVER_NAME") or ""
        else:
            logical_name_filter = sql_defaults["logical_name"]
            database_name_filter = sql_defaults["database_name"]
            schema_name_filter = sql_defaults["schema_name"]
            server_name_filter = sql_defaults["server_name"]

        with st.expander("Advanced options", expanded=False):
            if schedule_mode == "Scheduled":
                col_start_time, col_stop_time = st.columns(2)
                start_time = col_start_time.time_input("Start Time", value=time(hour=0, minute=0))
                stop_time = col_stop_time.time_input("Stop Time", value=time(hour=23, minute=59))
                schedule_start_dt = datetime.combine(start_date, start_time)
                schedule_stop_dt = datetime.combine(stop_date, stop_time)
            logical_name = st.text_input(
                "Logical Name",
                value=logical_name_filter,
            )
            col_db, col_schema = st.columns(2)
            database_name = col_db.text_input(
                "Source Database",
                value=database_name_filter,
                help=f"Observed in metadata: {', '.join(SQL_SERVER_DATABASE_EXAMPLES)}",
            )
            schema_name = col_schema.text_input(
                "Source Schema",
                value=schema_name_filter,
                help="Choose A3_CURR for AgentCubed, dbo for Genesys/Mindful workloads.",
            )
            server_name = st.text_input(
                "SQL Server Name",
                value=server_name_filter,
                help=f"Other observed servers: {', '.join(SQL_SERVER_SERVER_EXAMPLES[1:])}.",
            )
            priority_flag = st.checkbox(
                "High priority run",
                value=sql_defaults["priority_flag"],
                help="Bumps the request to the top of the queue when checked.",
            )

            delta_options = _load_delta_columns(
                conn,
                runtime_mode,
                logical_name=logical_name,
                database_name=database_name,
                schema_name=schema_name,
                server_name=server_name,
                source_table_name=source_table_name,
            )
            base_delta_options = delta_options or SQL_SERVER_DELTA_COLUMNS
            default_delta = base_delta_options[0] if base_delta_options else sql_defaults["delta_column"]
            change_tracking_options = _load_change_tracking(
                conn,
                runtime_mode,
                logical_name=logical_name,
                database_name=database_name,
                schema_name=schema_name,
                server_name=server_name,
                source_table_name=source_table_name,
            )
            base_change_tracking = change_tracking_options or SQL_SERVER_CHANGE_TRACKING
            col_delta, col_ct = st.columns(2)
            try:
                delta_default_index = base_delta_options.index(default_delta)
            except ValueError:
                delta_default_index = 0
            delta_column_choice = col_delta.selectbox(
                "Delta Column",
                base_delta_options,
                index=delta_default_index,
            )
            default_change_tracking = base_change_tracking[0] if base_change_tracking else sql_defaults["change_tracking_type"]
            try:
                ct_default_index = base_change_tracking.index(default_change_tracking)
            except ValueError:
                ct_default_index = 0
            change_tracking_type = col_ct.selectbox(
                "Change Tracking Type",
                base_change_tracking,
                index=ct_default_index,
            )
            delta_column = delta_column_choice

        table_options = _load_source_tables(
            conn,
            runtime_mode,
            logical_name_filter,
            database_name_filter,
            schema_name_filter,
            server_name_filter,
        )
        if selected_source and not table_options:
            st.warning("No tables found for the selected SQL metadata.")
            table_options = ["(No tables found)"]
            default_index = 0

        debug_sql_source_tables = (
            "SELECT DISTINCT SOURCE_TABLE_NAME "
            f"FROM {debug_table_ref} "
            f"WHERE LOGICAL_NAME ILIKE '%{_sql_literal(logical_name_filter)}%' "
            f"AND DATABASE_NAME ILIKE '%{_sql_literal(database_name_filter)}%' "
            f"AND SCHEMA_NAME ILIKE '%{_sql_literal(schema_name_filter)}%' "
            f"AND COALESCE(SERVER_NAME, '') ILIKE '%{_sql_literal(server_name_filter)}%' "
            "ORDER BY SOURCE_TABLE_NAME"
        )

        with st.expander("Lookup queries", expanded=False):
            st.caption("Existing SQL metadata query")
            st.code(debug_sql_metadata, language="sql")
            st.caption("Source Table query")
            st.code(debug_sql_source_tables, language="sql")
            if not selected_source:
                fallback_tables = [sql_defaults["source_table_name"]]
                table_options = table_options or fallback_tables
                default_table = sql_defaults["source_table_name"]
                default_index = table_options.index(default_table) if default_table in table_options else 0

        source_table_name = st.selectbox(
            "Source Table",
            options=table_options,
            index=default_index,
            key="source_table_name_select",
            help="Reference the upstream SQL Server table or view.",
            disabled=selected_source is not None and table_options == ["(No tables found)"]
        )
        is_file_source = active_source_type in {"Flat File", "Excel File"}

        if is_file_source:
            col_file1, col_file2 = st.columns(2)
            file_landing_schema = col_file1.text_input(
                "Landing Schema (optional)",
                value=sql_defaults["file_landing_schema"],
                help="Populate when landing schemas differ from source.",
            )
            file_table_name = col_file2.text_input(
                "Landing Table (optional)",
                value=sql_defaults["file_table_name"],
                help="Override when STAGE tables follow naming that differs from the source.",
            )

            col_file3, col_file4 = st.columns(2)
            file_wildcard_pattern = col_file3.text_input(
                "Source Pattern (optional)",
                value=sql_defaults["file_wildcard_pattern"],
                help="Pattern for staged files.",
            )
            file_extension = col_file4.text_input(
                "File Extension (optional)",
                value=sql_defaults["file_extension"],
                help="Leave blank to infer.",
            )

            col_header, col_format = st.columns(2)
            header_options = ["Y", "N", ""]
            header_index = header_options.index(sql_defaults["has_header"]) if sql_defaults["has_header"] in header_options else 0
            has_header = col_header.selectbox(
                "Header Row Present?",
                options=header_options,
                index=header_index,
            )
            file_format = sql_defaults["file_format"]
            col_format.text_input(
                "Snowflake File Format",
                value=file_format,
                disabled=True,
            )
        else:
            # SQL ingestion: file fields are suppressed
            file_landing_schema = ""
            file_table_name = ""
            file_wildcard_pattern = ""
            file_extension = ""
            has_header = ""
            file_format = sql_defaults["file_format"]

        env_upper = environment.upper()
        adhoc_proc_name = f"STAGE_{env_upper}.ELT.ADHOC_INSERT_METADATA_CONFIG_TABLE_{env_upper}"
        scheduled_proc_name = f"STAGE_{env_upper}.ELT.INSERT_METADATA_CONFIG_TABLE_{env_upper}"
        run_now = adhoc_allowed and schedule_mode == "Adhoc"
        schedule_enable = schedule_mode == "Scheduled"
        if schedule_mode == "Adhoc" and not adhoc_allowed:
            st.warning("Adhoc metadata table is not deployed to this environment; switching to scheduled ingestion.")
            run_now = False
            schedule_enable = True
        proc_col1, proc_col2 = st.columns(2)
        proc_col1.caption(f"Adhoc procedure: {adhoc_proc_name}")
        proc_col2.caption(f"Scheduled procedure: {scheduled_proc_name}")

        enabled_flag = st.toggle(
            "Enable ingestion",
            value=True,
            help="When on, ENABLED is set to '1'; when off, it is set to '0'.",
        )

        col_submit, col_show = st.columns([1, 1])
        with col_submit:
            generate_sql = st.form_submit_button("Generate SQL")
        with col_show:
            show_last_runs = st.form_submit_button("Show last runs")

        # If the user asked to show last runs, run that query and return early.
        if show_last_runs:
            tbl = (source_table_name or "").strip()
            if runtime_mode == "duckdb":
                # Detect which local tables exist and build an appropriate query.
                adhoc_table = "METADATA_CONFIG_TABLE_ELT_ADHOC"
                audit_candidates = ["AUDIT_LOG", "ELT_LOAD_LOG"]

                adhoc_exists = _table_exists(conn, adhoc_table, runtime_mode)
                audit_found = None
                for cand in audit_candidates:
                    if _table_exists(conn, cand, runtime_mode):
                        audit_found = cand
                        break

                if adhoc_exists and audit_found:
                    sql = f"""
SELECT
    m.METADATA_CONFIG_KEY,
    m.LOGICAL_NAME,
    m.DATABASE_NAME AS SOURCE_DATABASE,
    m.SCHEMA_NAME AS SOURCE_SCHEMA,
    m.SOURCE_TABLE_NAME AS SOURCE_TABLE,
    'ADHOC' AS RUN_SOURCE,
    a.LOAD_START_DATETIME AS RUN_START_DATETIME,
    a.LOAD_END_DATETIME AS RUN_END_DATETIME,
    a.LAST_TRIGGER_TIMESTAMP AS RUN_TS
FROM METADATA_CONFIG_TABLE_ELT AS m
JOIN {adhoc_table} AS a
    ON m.METADATA_CONFIG_KEY = a.METADATA_CONFIG_KEY
WHERE UPPER(COALESCE(m.SOURCE_TABLE_NAME, '')) = UPPER('{tbl}')

UNION ALL

SELECT
    m.METADATA_CONFIG_KEY,
    m.LOGICAL_NAME,
    '' AS SOURCE_DATABASE,
    '' AS SOURCE_SCHEMA,
    COALESCE(al.LOAD_NAME, al.LOAD_ID, '') AS SOURCE_TABLE,
    'AUDIT' AS RUN_SOURCE,
    al.LOAD_START_TIME AS RUN_START_DATETIME,
    al.LOAD_END_TIME AS RUN_END_DATETIME,
    al.LOAD_START_TIME AS RUN_TS
FROM METADATA_CONFIG_TABLE_ELT AS m
JOIN {audit_found} AS al
    ON UPPER(COALESCE(m.SOURCE_TABLE_NAME, '')) = UPPER(COALESCE(al.LOAD_NAME, al.LOAD_ID, ''))
WHERE UPPER(COALESCE(al.LOAD_NAME, al.LOAD_ID, '')) = UPPER('{tbl}')
ORDER BY RUN_TS DESC
LIMIT 20
"""
                elif adhoc_exists:
                    sql = f"""
SELECT
    m.METADATA_CONFIG_KEY,
    m.LOGICAL_NAME,
    m.DATABASE_NAME AS SOURCE_DATABASE,
    m.SCHEMA_NAME AS SOURCE_SCHEMA,
    m.SOURCE_TABLE_NAME AS SOURCE_TABLE,
    'ADHOC' AS RUN_SOURCE,
    a.LOAD_START_DATETIME AS RUN_START_DATETIME,
    a.LOAD_END_DATETIME AS RUN_END_DATETIME,
    a.LAST_TRIGGER_TIMESTAMP AS RUN_TS
FROM METADATA_CONFIG_TABLE_ELT AS m
JOIN {adhoc_table} AS a
    ON m.METADATA_CONFIG_KEY = a.METADATA_CONFIG_KEY
WHERE UPPER(COALESCE(m.SOURCE_TABLE_NAME, '')) = UPPER('{tbl}')
ORDER BY RUN_TS DESC
LIMIT 20
"""
                elif audit_found:
                    sql = f"""
SELECT
    m.METADATA_CONFIG_KEY,
    m.LOGICAL_NAME,
    '' AS SOURCE_DATABASE,
    '' AS SOURCE_SCHEMA,
    COALESCE(al.LOAD_NAME, al.LOAD_ID, '') AS SOURCE_TABLE,
    'AUDIT' AS RUN_SOURCE,
    al.LOAD_START_TIME AS RUN_START_DATETIME,
    al.LOAD_END_TIME AS RUN_END_DATETIME,
    al.LOAD_START_TIME AS RUN_TS
FROM METADATA_CONFIG_TABLE_ELT AS m
JOIN {audit_found} AS al
    ON UPPER(COALESCE(m.SOURCE_TABLE_NAME, '')) = UPPER(COALESCE(al.LOAD_NAME, al.LOAD_ID, ''))
WHERE UPPER(COALESCE(al.LOAD_NAME, al.LOAD_ID, '')) = UPPER('{tbl}')
ORDER BY RUN_TS DESC
LIMIT 20
"""
                else:
                    st.info("No adhoc or audit run tables exist in this DuckDB test dataset. Run Test Data setup to seed them.")
                    return
            else:
                view_ref = "DATA_VAULT_TEMP.MIGRATION.METADATA_CONFIG_TABLE_ELT_ADHOC_VW"
                sql = (
                    f"SELECT METADATA_CONFIG_KEY, LOGICAL_NAME, DATABASE_NAME, SCHEMA_NAME, SOURCE_TABLE_NAME, "
                    f"LOAD_START_DATETIME, LOAD_END_DATETIME, LAST_TRIGGER_TIMESTAMP, ERROR_STATUS, ENVIRONMENT "
                    f"FROM {view_ref} "
                    f"WHERE UPPER(COALESCE(SOURCE_TABLE_NAME, '')) = UPPER('{tbl}') "
                    "ORDER BY LAST_TRIGGER_TIMESTAMP DESC LIMIT 20"
                )

            with st.expander("Preview query", expanded=False):
                st.code(sql, language="sql")

            if conn is None:
                st.info("Connect to Snowflake to run this query and see recent runs.")
                return

            try:
                df_runs = run_query(conn, sql, runtime_mode)
            except Exception as e:
                st.error(f"Failed to run recent-runs query: {e}")
                return

            if df_runs is None or df_runs.empty:
                st.info("No recent runs found for that table.")
                return

            st.subheader("Recent runs")
            st.dataframe(df_runs.fillna(""), hide_index=True)
            return

    if not generate_sql:
        return

    source_type_label = active_source_type
    sheet_name = sql_defaults["sheet_name"]
    file_server_load_type = sql_defaults["file_server_load_type"]
    filestamp_format = sql_defaults["filestamp_format"]
    auto_ingest = sql_defaults["auto_ingest"]
    file_server_filepath = sql_defaults["file_server_filepath"]
    fixed_width_filetype = sql_defaults["fixed_width_filetype"]
    regex_pattern = sql_defaults["regex_pattern"]
    env_upper = environment.upper()
    adhoc_proc_name = f"STAGE_{env_upper}.ELT.ADHOC_INSERT_METADATA_CONFIG_TABLE_{env_upper}"
    scheduled_proc_name = f"STAGE_{env_upper}.ELT.INSERT_METADATA_CONFIG_TABLE_{env_upper}"

    statements: StatementList = []
    adhoc_entries: StatementList = []
    reference_entries: StatementList = []
    
    if run_now and environment in ADHOC_ENVIRONMENTS:
        statements.append(
            (
                f"Adhoc metadata trigger ({adhoc_proc_name})",
                build_adhoc_insert_sql(
                    environment,
                    file_landing_schema=file_landing_schema,
                    file_wildcard_pattern=file_wildcard_pattern,
                    file_table_name=file_table_name,
                    file_extension=file_extension,
                    sheet_name=sheet_name,
                    logical_name=logical_name,
                    source_type_label=source_type_label,
                    enabled_flag=enabled_flag,
                    file_format=file_format,
                    has_header=has_header,
                    database_name=database_name,
                    schema_name=schema_name,
                    source_table_name=source_table_name,
                    delta_column=delta_column,
                    change_tracking_type=change_tracking_type,
                    server_name=server_name,
                    file_server_load_type=file_server_load_type,
                    filestamp_format=filestamp_format,
                    auto_ingest=auto_ingest,
                    file_server_filepath=file_server_filepath,
                    fixed_width_filetype=fixed_width_filetype,
                    regex_pattern=regex_pattern,
                    requested_by=_get_requester(),
                    notes="",
                    priority_flag=priority_flag,
                ),
            )
        )
    elif run_now and environment not in ADHOC_ENVIRONMENTS:
        st.warning("Adhoc metadata table is not deployed to PROD.")

    if schedule_enable:
        # Generate SET ... CALL statements for the scheduled ingestion procedure
        # Build SET lines and CALL statement separately so the UI shows the SETs
        # before the CALL (filled with literal values).
        set_sql, call_sql = build_schedule_sets_and_call(
            environment,
            logical_name=logical_name,
            file_landing_schema=file_landing_schema,
            file_wildcard_pattern=file_wildcard_pattern,
            file_table_name=file_table_name,
            file_extension=file_extension,
            sheet_name=sheet_name,
            source_type_label=source_type_label,
            enabled_flag=enabled_flag,
            file_format=file_format,
            has_header=has_header,
            database_name=database_name,
            schema_name=schema_name,
            source_table_name=source_table_name,
            delta_column=delta_column,
            change_tracking_type=change_tracking_type,
            server_name=server_name,
            file_server_load_type=file_server_load_type,
            filestamp_format=filestamp_format,
            auto_ingest=auto_ingest,
            file_server_filepath=file_server_filepath,
            fixed_width_filetype=fixed_width_filetype,
            regex_pattern=regex_pattern,
        )
        # Build ordered entries: adhoc SET/CALL (if applicable) first,
        # then scheduled SET/CALL, then the full scheduled metadata SQL.
        adhoc_entries: StatementList = []
        reference_entries: StatementList = []

        # adhoc entries (for DEV/TEST/UAT) - these are executable
        if environment in ADHOC_ENVIRONMENTS:
            adhoc_key = str(uuid.uuid4())
            last_trigger = datetime.now()
            load_start_literal = schedule_start_dt.isoformat() if schedule_start_dt else ""
            load_end_literal = schedule_stop_dt.isoformat() if schedule_stop_dt else ""
            adhoc_set = "\n".join([
                f"SET METADATA_CONFIG_KEY       = '{adhoc_key}';",
                f"SET LOAD_START_DATETIME       = '{load_start_literal}';",
                f"SET LOAD_END_DATETIME         = '{load_end_literal}';",
                f"SET LAST_TRIGGER_TIMESTAMP    = '{last_trigger.isoformat()}';",
            ])
            adhoc_proc_name = f"STAGE_{environment.upper()}.ELT.INSERT_METADATA_CONFIG_TABLE_ADHOC_{environment.upper()}"
            adhoc_call = (
                f"CALL {adhoc_proc_name}(\n"
                "    $METADATA_CONFIG_KEY,\n"
                "    $LOAD_START_DATETIME,\n"
                "    $LOAD_END_DATETIME,\n"
                "    $LAST_TRIGGER_TIMESTAMP\n"
                ");"
            )
            adhoc_entries.append((f"SET variables for {adhoc_proc_name}", adhoc_set))
            adhoc_entries.append((f"Call adhoc trigger ({adhoc_proc_name})", adhoc_call))

        # scheduled SET/CALL - reference only
        reference_entries.append((f"SET variables for {scheduled_proc_name}", set_sql))
        reference_entries.append((f"Call scheduled procedure ({scheduled_proc_name})", call_sql))

        # full metadata SQL for reference
        full_sql = build_schedule_update_sql(
            environment,
            logical_name=logical_name,
            file_landing_schema=file_landing_schema,
            file_wildcard_pattern=file_wildcard_pattern,
            file_table_name=file_table_name,
            file_extension=file_extension,
            sheet_name=sheet_name,
            source_type_label=source_type_label,
            enabled_flag=enabled_flag,
            file_format=file_format,
            has_header=has_header,
            database_name=database_name,
            schema_name=schema_name,
            source_table_name=source_table_name,
            delta_column=delta_column,
            change_tracking_type=change_tracking_type,
            server_name=server_name,
            file_server_load_type=file_server_load_type,
            filestamp_format=filestamp_format,
            auto_ingest=auto_ingest,
            file_server_filepath=file_server_filepath,
            fixed_width_filetype=fixed_width_filetype,
            regex_pattern=regex_pattern,
            requested_by=_get_requester(),
            notes=(
                f"ScheduleStart={schedule_start_dt.isoformat()}|ScheduleStop={schedule_stop_dt.isoformat()}"
                if schedule_start_dt and schedule_stop_dt
                else ""
            ),
            priority_flag=priority_flag,
        )
        reference_entries.append((f"Enable scheduled ingestion ({scheduled_proc_name})", full_sql))

        # Combine: adhoc first (executable), then reference
        statements = adhoc_entries + reference_entries

    if not statements:
        st.info("No SQL generated. Enable at least one action above.")
        return

    # Display adhoc statements with Run SQL button
    if adhoc_entries:
        st.markdown("### Adhoc Trigger SQL")
        for label, sql in adhoc_entries:
            st.markdown(f"**{label}**")
            st.code(sql, language="sql")
        
        if conn is None:
            st.info("Connect to Snowflake or DuckDB to execute this SQL.")
        else:
            col_exec, col_view = st.columns(2)
            with col_exec:
                execute_now = st.button("Run SQL", type="primary", key="exec_copilot_sql")
            with col_view:
                show_adhoc = st.button("Show adhoc table", key="show_adhoc_table")

            # Show adhoc table on demand
            if show_adhoc:
                env_upper = environment.upper()
                if runtime_mode == "duckdb":
                    adhoc_table_ref = "METADATA_CONFIG_TABLE_ELT_ADHOC"
                else:
                    adhoc_table_ref = f"STAGE_{env_upper}.ELT.METADATA_CONFIG_TABLE_ELT_ADHOC"
                
                query_adhoc = f"SELECT * FROM {adhoc_table_ref} ORDER BY LAST_TRIGGER_TIMESTAMP DESC LIMIT 50"
                try:
                    df_adhoc = run_query(conn, query_adhoc, runtime_mode)
                    if df_adhoc is not None and not df_adhoc.empty:
                        st.subheader(f"Recent adhoc triggers ({len(df_adhoc)} rows)")
                        st.dataframe(df_adhoc, hide_index=True, width="stretch")
                    else:
                        st.info("No adhoc triggers found in this environment.")
                except Exception as e:
                    st.warning(f"Could not fetch adhoc table: {e}")
                return

            if execute_now:
                with st.spinner("Executing adhoc trigger SQL..."):
                    # Execute only the adhoc statements
                    for label, sql in adhoc_entries:
                        ok, msg = execute_ddl_dml(conn, runtime_mode, sql)
                        if ok:
                            st.success(f"{label} executed successfully.")
                        else:
                            st.error(f"{label} failed: {msg}")
                            break
                
                # Display adhoc table after execution
                st.divider()
                env_upper = environment.upper()
                if runtime_mode == "duckdb":
                    adhoc_table_ref = "METADATA_CONFIG_TABLE_ELT_ADHOC"
                else:
                    adhoc_table_ref = f"STAGE_{env_upper}.ELT.METADATA_CONFIG_TABLE_ELT_ADHOC"
                
                query_adhoc = f"SELECT * FROM {adhoc_table_ref} ORDER BY LAST_TRIGGER_TIMESTAMP DESC LIMIT 50"
                try:
                    df_adhoc = run_query(conn, query_adhoc, runtime_mode)
                    if df_adhoc is not None and not df_adhoc.empty:
                        st.subheader(f"Recent adhoc triggers ({len(df_adhoc)} rows)")
                        st.dataframe(df_adhoc, hide_index=True, width="stretch")
                    else:
                        st.info("No adhoc triggers found in this environment.")
                except Exception as e:
                    st.warning(f"Could not fetch adhoc table: {e}")
                return

    # Display reference statements (scheduled procedure)
    if reference_entries:
        st.divider()
        st.markdown("### Reference SQL (Scheduled Procedure)")
        st.caption("These statements are for reference only. Copy and run manually in Snowflake if needed.")
        for label, sql in reference_entries:
            with st.expander(label, expanded=False):
                st.code(sql, language="sql")


def _render_table_lookup(conn: Any, runtime_mode: str) -> None:
    st.subheader("Table Lookup")
    st.caption("Filter SQL Server metadata rows by Logical Name, Server, Database, Schema, and Source Table.")

    if conn is None:
        st.info("Connect to Snowflake or run locally with DuckDB test data to explore the table catalog.")
        return

    prefix = "" if runtime_mode == "duckdb" else get_snowflake_table_prefix()
    table_ref = f"{prefix}.METADATA_SQL_SERVER_LOOKUP_VW" if prefix else "METADATA_SQL_SERVER_LOOKUP_VW"
    query = (
        "SELECT LOGICAL_NAME, SERVER_NAME, DATABASE_NAME, SCHEMA_NAME, SOURCE_TABLE_NAME, SOURCE_TYPE "
        f"FROM {table_ref} ORDER BY LOGICAL_NAME, DATABASE_NAME, SCHEMA_NAME, SOURCE_TABLE_NAME"
    )

    try:
        df = run_query(conn, query, runtime_mode)
    except Exception:
        st.warning("Table lookup view not available yet. Run test data setup to create the mock table.")
        return

    if df is None or df.empty:
        st.info("No rows available. Run the Test Data setup to seed the lookup table.")
        return

    df = df.fillna("")

    def _options(series: Any) -> List[str]:
        values = sorted({str(val) for val in series.tolist() if str(val).strip()})
        return ["All"] + values

    col1, col2, col3 = st.columns(3)
    col4, col5 = st.columns(2)

    logical_filter = col1.selectbox("Logical Name", _options(df["LOGICAL_NAME"]), index=0)
    server_filter = col2.selectbox("Server Name", _options(df["SERVER_NAME"]), index=0)
    database_filter = col3.selectbox("Database Name", _options(df["DATABASE_NAME"]), index=0)
    schema_filter = col4.selectbox("Schema_Name", _options(df["SCHEMA_NAME"]), index=0)
    source_table_filter = col5.selectbox("Source_Table_Name", _options(df["SOURCE_TABLE_NAME"]), index=0)

    filtered = df.copy()
    if logical_filter != "All":
        filtered = filtered[filtered["LOGICAL_NAME"] == logical_filter]
    if server_filter != "All":
        filtered = filtered[filtered["SERVER_NAME"] == server_filter]
    if database_filter != "All":
        filtered = filtered[filtered["DATABASE_NAME"] == database_filter]
    if schema_filter != "All":
        filtered = filtered[filtered["SCHEMA_NAME"] == schema_filter]
    if source_table_filter != "All":
        filtered = filtered[filtered["SOURCE_TABLE_NAME"] == source_table_filter]

    st.caption(f"Showing {len(filtered)} of {len(df)} rows")
    st.dataframe(filtered, hide_index=True)


def main() -> None:
    conn = init_page()
    runtime_mode = get_runtime_mode()

    st.title("🤖 Ingestion Copilot")
    st.markdown("Guide developers through configuring new ingestion sources across file, SQL, Azure, and Excel patterns.")

    _render_ingestion_builder(conn, runtime_mode)
    st.divider()
    
    # Show all scheduled runs from the adhoc view
    st.subheader("📅 All Scheduled Runs")
    st.caption("Recent adhoc triggers across all environments (from METADATA_CONFIG_TABLE_ELT_ADHOC_VW)")
    
    if conn is None:
        st.info("Connect to Snowflake or DuckDB to view scheduled runs.")
    else:
        if runtime_mode == "duckdb":
            view_ref = "METADATA_CONFIG_TABLE_ELT_ADHOC_VW"
            # Check if view exists (will be created when test data is set up)
            if not _table_exists(conn, view_ref, runtime_mode):
                st.info("Adhoc view not yet created. Use 'Setup Test Data' button in sidebar to create views and tables.")
            else:
                query_all_runs = (
                    f"SELECT METADATA_CONFIG_KEY, LOGICAL_NAME, DATABASE_NAME, SCHEMA_NAME, SOURCE_TABLE_NAME, "
                    f"LOAD_START_DATETIME, LOAD_END_DATETIME, LAST_TRIGGER_TIMESTAMP, ERROR_STATUS, ENVIRONMENT "
                    f"FROM {view_ref} "
                    "ORDER BY LAST_TRIGGER_TIMESTAMP DESC LIMIT 100"
                )
                try:
                    df_all_runs = run_query(conn, query_all_runs, runtime_mode)
                    if df_all_runs is not None and not df_all_runs.empty:
                        st.caption(f"Showing {len(df_all_runs)} most recent triggers")
                        st.dataframe(df_all_runs, hide_index=True, width="stretch", height=400)
                    else:
                        st.info("No scheduled runs found. Use 'Run SQL' button to create adhoc triggers.")
                except Exception as e:
                    st.warning(f"Could not fetch scheduled runs: {e}")
        else:
            view_ref = "DATA_VAULT_TEMP.MIGRATION.METADATA_CONFIG_TABLE_ELT_ADHOC_VW"
            query_all_runs = (
                f"SELECT METADATA_CONFIG_KEY, LOGICAL_NAME, DATABASE_NAME, SCHEMA_NAME, SOURCE_TABLE_NAME, "
                f"LOAD_START_DATETIME, LOAD_END_DATETIME, LAST_TRIGGER_TIMESTAMP, ERROR_STATUS, ENVIRONMENT "
                f"FROM {view_ref} "
                "ORDER BY LAST_TRIGGER_TIMESTAMP DESC LIMIT 100"
            )
            
            try:
                df_all_runs = run_query(conn, query_all_runs, runtime_mode)
                if df_all_runs is not None and not df_all_runs.empty:
                    st.caption(f"Showing {len(df_all_runs)} most recent triggers")
                    st.dataframe(df_all_runs, hide_index=True, width="stretch", height=400)
                else:
                    st.info("No scheduled runs found. Create adhoc triggers using the builder above.")
            except Exception as e:
                st.warning(f"Could not fetch scheduled runs: {e}")
    
    st.divider()
    _render_table_lookup(conn, runtime_mode)


main()
