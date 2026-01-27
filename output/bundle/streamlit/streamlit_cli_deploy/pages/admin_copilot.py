"""Ingestion Copilot page with build/run workflow."""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

import streamlit as st

from common.layout import init_page, get_runtime_mode
from common.db import execute_ddl_dml, get_snowflake_table_prefix, run_query
from common.ingestion_templates import (
    ADHOC_ENVIRONMENTS,
    ADF_POLL_SCHEDULE,
    INGESTION_ENVIRONMENTS,
    build_adhoc_insert_sql,
    build_schedule_update_sql,
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

    view_name = "METADATA_SQL_SOURCES_PICKLIST_VW"
    prefix = "" if runtime_mode == "duckdb" else get_snowflake_table_prefix()
    table_ref = f"{prefix}.{view_name}" if prefix else view_name
    query = (
        "SELECT LOGICAL_NAME, DATABASE_NAME, SCHEMA_NAME, SERVER_NAME, SOURCE_LABEL "
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
) -> List[str]:
    if conn is None:
        return []

    view_name = "METADATA_SQL_DELTA_COLUMNS_VW"
    prefix = "" if runtime_mode == "duckdb" else get_snowflake_table_prefix()
    table_ref = f"{prefix}.{view_name}" if prefix else view_name

    query = (
        "SELECT DISTINCT DELTA_COLUMN FROM "
        f"{table_ref} "
        "WHERE LOGICAL_NAME = %(logical)s "
        "AND DATABASE_NAME = %(db)s "
        "AND SCHEMA_NAME = %(schema)s "
        "AND COALESCE(SERVER_NAME, '') = COALESCE(%(server)s, '') "
        "ORDER BY DELTA_COLUMN"
    )

    try:
        df = run_query(
            conn,
            query % {
                "logical": logical_name,
                "db": database_name,
                "schema": schema_name,
                "server": server_name,
            },
            runtime_mode,
        )
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
) -> List[str]:
    if conn is None:
        return []

    view_name = "METADATA_SQL_CHANGE_TRACKING_VW"
    prefix = "" if runtime_mode == "duckdb" else get_snowflake_table_prefix()
    table_ref = f"{prefix}.{view_name}" if prefix else view_name

    query = (
        "SELECT DISTINCT CHANGE_TRACKING_TYPE FROM "
        f"{table_ref} "
        "WHERE LOGICAL_NAME = %(logical)s "
        "AND DATABASE_NAME = %(db)s "
        "AND SCHEMA_NAME = %(schema)s "
        "AND COALESCE(SERVER_NAME, '') = COALESCE(%(server)s, '') "
        "ORDER BY CHANGE_TRACKING_TYPE"
    )

    try:
        df = run_query(
            conn,
            query % {
                "logical": logical_name,
                "db": database_name,
                "schema": schema_name,
                "server": server_name,
            },
            runtime_mode,
        )
    except Exception:
        return []

    if df is None or df.empty:
        return []
    return sorted({str(v) for v in df["CHANGE_TRACKING_TYPE"].dropna().tolist()})


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

        source_table_name = st.text_input(
            "Source Table",
            value=sql_defaults["source_table_name"],
            help="Reference the upstream SQL Server table or view.",
        )

        with st.expander("Advanced options", expanded=False):
            logical_name = st.text_input(
                "Logical Name",
                value=(selected_source.get("LOGICAL_NAME") or sql_defaults["logical_name"]) if selected_source else sql_defaults["logical_name"],
            )
            col_db, col_schema = st.columns(2)
            database_name = col_db.text_input(
                "Source Database",
                value=(selected_source.get("DATABASE_NAME") or sql_defaults["database_name"]) if selected_source else sql_defaults["database_name"],
                help=f"Observed in metadata: {', '.join(SQL_SERVER_DATABASE_EXAMPLES)}",
            )
            schema_name = col_schema.text_input(
                "Source Schema",
                value=(selected_source.get("SCHEMA_NAME") or sql_defaults["schema_name"]) if selected_source else sql_defaults["schema_name"],
                help="Choose A3_CURR for AgentCubed, dbo for Genesys/Mindful workloads.",
            )
            server_name = st.text_input(
                "SQL Server Name",
                value=(selected_source.get("SERVER_NAME") or sql_defaults["server_name"]) if selected_source else sql_defaults["server_name"],
                help=f"Other observed servers: {', '.join(SQL_SERVER_SERVER_EXAMPLES[1:])}.",
            )
            priority_flag = st.checkbox(
                "High priority run",
                value=sql_defaults["priority_flag"],
                help="Bumps the request to the top of the queue when checked.",
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

        delta_options = _load_delta_columns(
            conn,
            runtime_mode,
            logical_name=logical_name,
            database_name=database_name,
            schema_name=schema_name,
            server_name=server_name,
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
        try:
            ct_default_index = base_change_tracking.index(sql_defaults["change_tracking_type"])
        except ValueError:
            ct_default_index = 0
        change_tracking_type = col_ct.selectbox(
            "Change Tracking Type",
            base_change_tracking,
            index=ct_default_index,
        )
        delta_column = delta_column_choice
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

        submitted = st.form_submit_button("Generate SQL")

    if not submitted:
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
        statements.append(
            (
                f"Enable scheduled ingestion ({scheduled_proc_name})",
                build_schedule_update_sql(
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
                    notes="",
                    priority_flag=priority_flag,
                ),
            )
        )

    if not statements:
        st.info("No SQL generated. Enable at least one action above.")
        return

    st.markdown("### Generated SQL")
    for label, sql in statements:
        st.markdown(f"**{label}**")
        st.code(sql, language="sql")

    if conn is None or runtime_mode not in {"snowflake_local", "snowflake_deployed"}:
        st.info("Preview only. Connect to Snowflake to execute these statements directly.")
        return

    if st.button("Execute SQL in Snowflake", type="primary"):
        for label, sql in statements:
            ok, msg = execute_ddl_dml(conn, runtime_mode, sql)
            if ok:
                st.success(f"{label} executed successfully.")
            else:
                st.error(f"{label} failed: {msg}")
                break


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
    _render_table_lookup(conn, runtime_mode)


main()
