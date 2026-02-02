"""
Reporting View Creation

Stub UI to generate reporting views in DATA_VAULT_DEV.INFO_MART selecting from
corresponding STAGE_<ENV> sources.
"""

from __future__ import annotations

import streamlit as st
from typing import Any

from common.layout import init_page, get_runtime_mode
from common.db import run_query


DEFAULT_ENV = "DEV"
DEFAULT_VIEW_DB = "DATA_VAULT_DEV"
DEFAULT_VIEW_SCHEMA = "INFO_MART"
DEFAULT_SOURCE_SCHEMA = "ELT"


@st.cache_data(ttl=300)
def _fetch_schemas(database: str, conn: Any) -> list[str]:
    """Fetch all schemas from a given database using INFORMATION_SCHEMA."""
    if conn is None:
        return [DEFAULT_SOURCE_SCHEMA]

    query = (
        f"SELECT SCHEMA_NAME "
        f"FROM \"{database}\".INFORMATION_SCHEMA.SCHEMATA "
        f"ORDER BY SCHEMA_NAME;"
    )
    try:
        mode = get_runtime_mode()
        result = run_query(conn, query, mode)
        if result is not None and len(result) > 0:
            return result["SCHEMA_NAME"].tolist()
    except Exception:
        pass
    
    return [DEFAULT_SOURCE_SCHEMA]


def _build_view_sql(env: str, source_table: str, view_name: str, source_schema: str) -> str:
    env_upper = env.upper()
    source_db = f"STAGE_{env_upper}"
    view_db = DEFAULT_VIEW_DB
    view_schema = DEFAULT_VIEW_SCHEMA
    src_schema = source_schema or DEFAULT_SOURCE_SCHEMA
    src_fqn = f'"{source_db}"."{src_schema}"."{source_table}"'
    view_fqn = f'"{view_db}"."{view_schema}"."{view_name}"'
    return (
        f"CREATE OR REPLACE VIEW {view_fqn} AS\n"
        f"SELECT * FROM {src_fqn};"
    )


def main() -> None:
    conn = init_page()

    st.title("🧾 Reporting View Creation")
    st.markdown(
        "Generate reporting views in DATA_VAULT_DEV.INFO_MART that select from the"
        " corresponding STAGE_<ENV> sources. This is a stub — edit fields and use"
        " the SQL preview below. Execution is disabled for now."
    )

    if conn is None:
        st.info("Connect to Snowflake to execute; preview still works.")

    # Select environment first
    env = st.selectbox(
        "Environment",
        ["DEV", "TEST", "UAT", "PROD"],
        index=0,
        help="Select the target environment. This determines the source database (STAGE_<ENV>).",
    )

    # Fetch schemas from the selected environment database
    env_upper = env.upper()
    source_db = f"STAGE_{env_upper}"
    available_schemas = _fetch_schemas(source_db, conn) if conn is not None else [DEFAULT_SOURCE_SCHEMA]

    with st.form("reporting_view_form"):
        col1, col2 = st.columns(2)
        source_table = col1.text_input(
            "Source Table",
            value="MY_SOURCE_TABLE",
            help="The name of the table in the source STAGE database to select from.",
        )
        source_schema = col2.selectbox(
            "Source Schema",
            options=available_schemas,
            help="Select a schema from the STAGE database.",
            key="source_schema_select",
        )
        view_name = st.text_input(
            "View Name",
            value="MY_REPORTING_VIEW",
            help="The name for the new reporting view in DATA_VAULT_DEV.INFO_MART.",
        )
        submitted = st.form_submit_button("Preview SQL")

    if not submitted:
        return

    sql = _build_view_sql(env, source_table, view_name, source_schema)

    st.markdown("### Generated SQL")
    st.code(sql, language="sql")

    st.info("Execution stubbed: wire up run_query/execute_ddl_dml once sources are ready.")


main()
