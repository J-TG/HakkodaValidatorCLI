"""
Projection Creation Builder

Reads source-to-target mappings and generates Snowflake projections (views)
that mimic legacy EDW tables while pointing at new Snowflake targets.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

import pandas as pd
import streamlit as st

from common.layout import init_page, get_runtime_mode
from common.db import (
    get_snowflake_table_prefix,
    run_query,
    execute_ddl_dml,
)


@dataclass
class SchemaConfig:
    database: str
    schema: str

    def fq_table(self, table: str, mode: Optional[str] = None) -> str:
        table_ident = quote_ident(table)
        if mode == "duckdb":
            return table_ident
        return f'{quote_ident(self.database)}.{quote_ident(self.schema)}.{table_ident}'

    def snowflake_fq_table(self, table: str) -> str:
        """Fully qualified name with database and schema for display purposes."""
        table_ident = quote_ident(table)
        return f'{quote_ident(self.database)}.{quote_ident(self.schema)}.{table_ident}'

    def label(self) -> str:
        return f"{self.database}.{self.schema}"


def quote_ident(name: str) -> str:
    """Quote an identifier for Snowflake (double quotes, escape internal quotes)."""
    safe = (name or "").strip()
    escaped = safe.replace('"', '""')
    return f'"{escaped}"'


def ensure_config_defaults() -> None:
    default_prefix = get_snowflake_table_prefix()
    default_db, default_schema = (default_prefix.split(".") + ["INFO_MART"])[:2]
    st.session_state.setdefault("compat_mapping_db", "DATA_VAULT_TEMP")
    st.session_state.setdefault("compat_mapping_schema", "MIGRATION")
    st.session_state.setdefault("compat_finance_mapping", "DATA_VAULT_DEV.INFO_MART.SCANDS_FINANCE_S2T")
    st.session_state.setdefault("compat_prod_mapping", "DATA_VAULT_DEV.INFO_MART.SCANDS_PROD_S2T")
    st.session_state.setdefault("compat_quality_mapping", "DATA_VAULT_DEV.INFO_MART.SCANDS_QUALITYRISK_S2T")
    st.session_state.setdefault("compat_target_db", "STAGE_TEST")
    st.session_state.setdefault("compat_view_db", "DATA_VAULT_TEMP")
    st.session_state.setdefault("compat_view_schema", "INFO_MART")
    # Environment database configuration
    st.session_state.setdefault("env_db_dev", "STAGE_DEV")
    st.session_state.setdefault("env_db_test", "STAGE_TEST")
    st.session_state.setdefault("env_db_uat", "STAGE_UAT")
    st.session_state.setdefault("env_db_prod", "STAGE_PROD")


def get_mapping_config() -> SchemaConfig:
    return SchemaConfig(
        st.session_state.get("compat_mapping_db", "DATA_VAULT_TEMP").strip() or "DATA_VAULT_TEMP",
        st.session_state.get("compat_mapping_schema", "MIGRATION").strip() or "MIGRATION",
    )


def get_domain_schemas() -> Dict[str, str]:
    """Extract database.schema from fully qualified mapping paths."""
    result = {}
    for domain, key in [("FINANCE", "compat_finance_mapping"), ("PROD", "compat_prod_mapping"), ("QUALITYRISK", "compat_quality_mapping")]:
        fqn = (st.session_state.get(key) or "").strip()
        if fqn and "." in fqn:
            parts = fqn.split(".")
            if len(parts) >= 2:
                result[domain] = f"{parts[0]}.{parts[1]}"
    return result


def get_target_db() -> str:
    return (st.session_state.get("compat_target_db") or "STAGE_TEST").strip()


def get_view_config() -> SchemaConfig:
    return SchemaConfig(
        st.session_state.get("compat_view_db", "DATA_VAULT_TEMP").strip() or "DATA_VAULT_TEMP",
        st.session_state.get("compat_view_schema", "INFO_MART").strip() or "INFO_MART",
    )


def get_environment_databases() -> Dict[str, str]:
    """Get the raw data database mapping for each environment."""
    return {
        "DEV": st.session_state.get("env_db_dev", "STAGE_DEV").strip(),
        "TEST": st.session_state.get("env_db_test", "STAGE_TEST").strip(),
        "UAT": st.session_state.get("env_db_uat", "STAGE_UAT").strip(),
        "PROD": st.session_state.get("env_db_prod", "STAGE_PROD").strip(),
    }


def create_mapping_view(conn, mode, mapping_cfg: SchemaConfig) -> tuple[bool, str]:
    db = mapping_cfg.database
    view_fqn = mapping_cfg.fq_table("SOURCE_TO_TARGET_MAPPING_VW", mode)
    
    # Get FQN mappings from session state
    finance_fqn = (st.session_state.get("compat_finance_mapping") or "DATA_VAULT_DEV.INFO_MART.SCANDS_FINANCE_S2T").strip()
    prod_fqn = (st.session_state.get("compat_prod_mapping") or "DATA_VAULT_DEV.INFO_MART.SCANDS_PROD_S2T").strip()
    quality_fqn = (st.session_state.get("compat_quality_mapping") or "DATA_VAULT_DEV.INFO_MART.SCANDS_QUALITYRISK_S2T").strip()
    
    # Convert to properly quoted identifiers if not in DuckDB mode
    if mode != "duckdb":
        finance = ".".join([quote_ident(part) for part in finance_fqn.split(".")])
        prod = ".".join([quote_ident(part) for part in prod_fqn.split(".")])
        quality = ".".join([quote_ident(part) for part in quality_fqn.split(".")])
    else:
        # DuckDB: use just table names
        finance = quote_ident(finance_fqn.split(".")[-1])
        prod = quote_ident(prod_fqn.split(".")[-1])
        quality = quote_ident(quality_fqn.split(".")[-1])
    
    select_columns = (
        "SOURCE_SCHEMA_NAME, SOURCE_TABLE_NAME, SOURCE_COLUMN_NAME, ORDINAL_POSITION, "
        "SOURCE_DATA_TYPE, MAX_LENGTH, PRECISION, SCALE, IS_NULLABLE, "
        "TARGET_TABLE_NAME, TARGET_COLUMN_NAME, TARGET_DATA_TYPE, "
        "SOURCE_DATABASE, TARGET_DATABASE"
    )
    view_sql = f"""
CREATE OR REPLACE VIEW {view_fqn} AS
SELECT 'FINANCE' AS DOMAIN, {select_columns}
FROM {finance}
UNION ALL
SELECT 'PROD' AS DOMAIN, {select_columns}
FROM {prod}
UNION ALL
SELECT 'QUALITYRISK' AS DOMAIN, {select_columns}
FROM {quality};
"""
    return execute_ddl_dml(conn, mode, view_sql)


def fetch_mapping_rows(conn, mode, mapping_cfg: SchemaConfig) -> pd.DataFrame:
    view_fqn = mapping_cfg.fq_table("SOURCE_TO_TARGET_MAPPING_VW", mode)
    query = (
        f"SELECT DOMAIN, SOURCE_SCHEMA_NAME, SOURCE_TABLE_NAME, SOURCE_COLUMN_NAME, ORDINAL_POSITION, "
        f"SOURCE_DATA_TYPE, TARGET_TABLE_NAME, TARGET_COLUMN_NAME, TARGET_DATA_TYPE, "
        f"SOURCE_DATABASE, TARGET_DATABASE "
        f"FROM {view_fqn} ORDER BY SOURCE_SCHEMA_NAME, SOURCE_TABLE_NAME, ORDINAL_POSITION"
    )
    return run_query(conn, query, mode)


def target_table_exists(conn, mode, view_cfg: SchemaConfig, table_name: str) -> tuple[bool, Optional[str]]:
    target_fqn = view_cfg.fq_table(table_name, mode)
    try:
        run_query(conn, f"SELECT 1 FROM {target_fqn} LIMIT 1;", mode)
        return True, None
    except Exception as exc:  # pragma: no cover - surfaced via UI
        return False, str(exc)


def build_view_sql(rows: pd.DataFrame, view_cfg: SchemaConfig, mode: str) -> str:
    target_table = rows["TARGET_TABLE_NAME"].iloc[0]
    source_table = rows["SOURCE_TABLE_NAME"].iloc[0]
    domain = (rows["DOMAIN"].iloc[0] or "").upper()
    order_rows = rows.sort_values("ORDINAL_POSITION")
    select_clauses: List[str] = []
    for _, row in order_rows.iterrows():
        target_col = row["TARGET_COLUMN_NAME"]
        source_col = row["SOURCE_COLUMN_NAME"]
        # Flip source/target so view columns match target names while sourcing from legacy columns
        select_clauses.append(
            f"    t.{quote_ident(source_col)} AS {quote_ident(target_col)}"
        )
    select_body = ",\n".join(select_clauses) if select_clauses else "    1"
    target_db = get_target_db()
    domain_schemas = get_domain_schemas()
    target_schema = domain_schemas.get(domain, view_cfg.schema)
    if mode == "duckdb":
        target_fqn = quote_ident(target_table)
    else:
        target_fqn = f"{quote_ident(target_db)}.{quote_ident(target_schema)}.{quote_ident(target_table)}"
    view_fqn = view_cfg.fq_table(source_table, mode)
    return (
        f"CREATE OR REPLACE VIEW {view_fqn} AS\n"
        f"SELECT\n{select_body}\nFROM {target_fqn} AS t;"
    )


def render_sql_preview(rows: pd.DataFrame, view_cfg: SchemaConfig, mode: str) -> None:
    if rows.empty:
        return
    target_table = rows["TARGET_TABLE_NAME"].iloc[0]
    view_table = rows["SOURCE_TABLE_NAME"].iloc[0]
    domain = (rows["DOMAIN"].iloc[0] or "").upper()

    # Use fully-qualified names for display, even in DuckDB mode
    domain_schemas = get_domain_schemas()
    target_db = get_target_db()
    target_schema = domain_schemas.get(domain, view_cfg.schema)
    target_fqn_display = f"{quote_ident(target_db)}.{quote_ident(target_schema)}.{quote_ident(target_table)}"
    view_fqn_display = view_cfg.snowflake_fq_table(view_table)

    # Execution SQL remains mode-aware; preview SQL shows Snowflake-qualified paths
    sql_exec = build_view_sql(rows, view_cfg, mode)
    preview_sql = sql_exec.replace(
        view_cfg.fq_table(view_table, mode), view_fqn_display
    ).replace(
        view_cfg.fq_table(target_table, mode), target_fqn_display
    )

    st.caption("Underlying target query")
    st.code(f"SELECT * FROM {target_fqn_display};", language="sql")
    st.caption("Generated view statement")
    st.code(preview_sql, language="sql")


def perform_view_action(action: str, tables: List[str], data: pd.DataFrame, conn, mode, view_cfg: SchemaConfig) -> List[tuple[str, bool, str]]:
    outcomes = []
    for table in tables:
        table_rows = data[data["SOURCE_TABLE_NAME"] == table]
        if table_rows.empty:
            outcomes.append((table, False, "No mapping rows available."))
            continue
        if action == "create":
            target_table = table_rows["TARGET_TABLE_NAME"].iloc[0]
            domain = (table_rows["DOMAIN"].iloc[0] or "").upper()
            target_db = get_target_db()
            domain_schemas = get_domain_schemas()
            target_schema = domain_schemas.get(domain, view_cfg.schema)
            if mode == "duckdb":
                target_fqn = quote_ident(target_table)
            else:
                target_fqn = f"{quote_ident(target_db)}.{quote_ident(target_schema)}.{quote_ident(target_table)}"
            try:
                run_query(conn, f"SELECT 1 FROM {target_fqn} LIMIT 1;", mode)
                exists, err = True, None
            except Exception as exc:  # pragma: no cover
                exists, err = False, str(exc)
            if not exists:
                outcomes.append((table, False, f"Target table {target_fqn} missing: {err}"))
                continue
            sql = build_view_sql(table_rows, view_cfg, mode)


def build_create_view_sql(source_rows: pd.DataFrame, source_env: str, view_cfg: SchemaConfig, mode: str, uppercase_columns: bool = False) -> str:
    """Build CREATE OR REPLACE VIEW statement for legacy column projection.
    
    Maps source columns to target columns with format:
    CREATE OR REPLACE VIEW <View DB>.<View Schema>.<Target Table> AS
    SELECT source_col AS target_col, ... FROM STAGE_<env>.<source_db>_<source_schema>.<source_table>
    
    Always generates fully qualified Snowflake syntax for display/execution.
    Columns preserve original case by default, or uppercase if uppercase_columns=True.
    Table/schema/database names are always uppercase.
    """
    if source_rows.empty:
        return ""
    
    # Get source info
    source_table = source_rows["SOURCE_TABLE_NAME"].iloc[0]
    source_schema = source_rows["SOURCE_SCHEMA_NAME"].iloc[0]
    source_db = source_rows["SOURCE_DATABASE"].iloc[0] if "SOURCE_DATABASE" in source_rows.columns else "UNKNOWN"
    target_table = source_rows["TARGET_TABLE_NAME"].iloc[0]
    
    # Build column mappings (source AS target) - apply uppercase if requested
    order_rows = source_rows.sort_values("ORDINAL_POSITION")
    select_clauses: List[str] = []
    for _, row in order_rows.iterrows():
        source_col = row["SOURCE_COLUMN_NAME"]
        target_col = row["TARGET_COLUMN_NAME"]
        if uppercase_columns:
            source_col = source_col.upper()
            target_col = target_col.upper()
        select_clauses.append(f"    {quote_ident(source_col)} AS {quote_ident(target_col)}")
    
    select_body = ",\n".join(select_clauses) if select_clauses else "    1"
    
    # Build source table reference with uppercase schema/database names
    # Format: STAGE_<env>.<source_db>_<source_schema>.<source_table>
    source_stage_db = f"STAGE_{source_env.upper()}"
    source_stage_schema = f"{source_db.upper()}_{source_schema.upper()}"
    source_table_upper = source_table.upper()
    
    # Build target view reference with uppercase schema/database names
    view_db_upper = view_cfg.database.upper()
    view_schema_upper = view_cfg.schema.upper()
    target_table_upper = target_table.upper()
    
    view_fqn = f"{quote_ident(view_db_upper)}.{quote_ident(view_schema_upper)}.{quote_ident(target_table_upper)}"
    source_fqn = f"{quote_ident(source_stage_db)}.{quote_ident(source_stage_schema)}.{quote_ident(source_table_upper)}"
    
    return (
        f"CREATE OR REPLACE VIEW {view_fqn} AS\n"
        f"SELECT\n{select_body}\n"
        f"FROM {source_fqn};"
    )


def perform_view_action(action: str, tables: List[str], data: pd.DataFrame, conn, mode, view_cfg: SchemaConfig) -> List[tuple[str, bool, str]]:
    outcomes = []
    for table in tables:
        table_rows = data[data["SOURCE_TABLE_NAME"] == table]
        if table_rows.empty:
            outcomes.append((table, False, "No mapping rows available."))
            continue
        if action == "create":
            target_table = table_rows["TARGET_TABLE_NAME"].iloc[0]
            domain = (table_rows["DOMAIN"].iloc[0] or "").upper()
            target_db = get_target_db()
            domain_schemas = get_domain_schemas()
            target_schema = domain_schemas.get(domain, view_cfg.schema)
            if mode == "duckdb":
                target_fqn = quote_ident(target_table)
            else:
                target_fqn = f"{quote_ident(target_db)}.{quote_ident(target_schema)}.{quote_ident(target_table)}"
            try:
                run_query(conn, f"SELECT 1 FROM {target_fqn} LIMIT 1;", mode)
                exists, err = True, None
            except Exception as exc:  # pragma: no cover
                exists, err = False, str(exc)
            if not exists:
                outcomes.append((table, False, f"Target table {target_fqn} missing: {err}"))
                continue
            sql = build_view_sql(table_rows, view_cfg, mode)
        else:
            sql = f"DROP VIEW IF EXISTS {view_cfg.fq_table(table, mode)};"
        ok, msg = execute_ddl_dml(conn, mode, sql)
        outcomes.append((table, ok, "Completed" if ok else msg))
    run_query.clear()
    return outcomes


def main() -> None:
    conn = init_page()
    mode = get_runtime_mode()
    ensure_config_defaults()
    mapping_cfg = get_mapping_config()
    view_cfg = get_view_config()
    snowflake_enabled = mode in ("snowflake_local", "snowflake_deployed")

    st.title("🧩 Projection Creation")
    st.markdown(
        f"Projections (views) are being built in `{view_cfg.label()}`"
    )

    # Environments section
    with st.expander("Environments", expanded=False):
        st.markdown("**Raw Data Databases** - This is where raw data is replicated to")
        with st.form("env_config_form"):
            col1, col2 = st.columns(2)
            with col1:
                env_dev = st.text_input(
                    "DEV",
                    value=st.session_state.get("env_db_dev", "STAGE_DEV"),
                    key="env_dev_input",
                )
                env_uat = st.text_input(
                    "UAT",
                    value=st.session_state.get("env_db_uat", "STAGE_UAT"),
                    key="env_uat_input",
                )
            with col2:
                env_test = st.text_input(
                    "TEST",
                    value=st.session_state.get("env_db_test", "STAGE_TEST"),
                    key="env_test_input",
                )
                env_prod = st.text_input(
                    "PROD",
                    value=st.session_state.get("env_db_prod", "STAGE_PROD"),
                    key="env_prod_input",
                )
            env_submitted = st.form_submit_button("Save Environments", type="secondary")
        if env_submitted:
            st.session_state["env_db_dev"] = env_dev.strip() or "STAGE_DEV"
            st.session_state["env_db_test"] = env_test.strip() or "STAGE_TEST"
            st.session_state["env_db_uat"] = env_uat.strip() or "STAGE_UAT"
            st.session_state["env_db_prod"] = env_prod.strip() or "STAGE_PROD"
            st.success("Environment databases updated.")

    with st.expander("Builder Configuration", expanded=False):
        with st.form("compat_config_form"):
            st.markdown("**Source-To-Target Mappings** - Fully qualified paths to S2T mapping tables")
            finance_mapping = st.text_input("Finance", value=st.session_state.get("compat_finance_mapping", "DATA_VAULT_DEV.INFO_MART.SCANDS_FINANCE_S2T"))
            prod_mapping = st.text_input("Prod", value=st.session_state.get("compat_prod_mapping", "DATA_VAULT_DEV.INFO_MART.SCANDS_PROD_S2T"))
            quality_mapping = st.text_input("Quality", value=st.session_state.get("compat_quality_mapping", "DATA_VAULT_DEV.INFO_MART.SCANDS_QUALITYRISK_S2T"))
            
            st.markdown("**Other Configuration**")
            env_options = get_environment_databases()
            env_list = list(env_options.keys())
            env_values = list(env_options.values())
            current_target = st.session_state.get("compat_target_db", "STAGE_TEST")
            try:
                default_index = env_values.index(current_target)
            except ValueError:
                default_index = 0
            selected_env = st.selectbox(
                "Source Environment",
                options=env_list,
                index=default_index,
                key="target_env_select",
            )
            target_db = env_options[selected_env]
            view_db = st.text_input("View Database", value=view_cfg.database)
            view_schema = st.text_input("View Schema", value=view_cfg.schema)
            submitted = st.form_submit_button("Save Configuration", type="secondary")
        if submitted:
            st.session_state["compat_finance_mapping"] = finance_mapping.strip() or "DATA_VAULT_DEV.INFO_MART.SCANDS_FINANCE_S2T"
            st.session_state["compat_prod_mapping"] = prod_mapping.strip() or "DATA_VAULT_DEV.INFO_MART.SCANDS_PROD_S2T"
            st.session_state["compat_quality_mapping"] = quality_mapping.strip() or "DATA_VAULT_DEV.INFO_MART.SCANDS_QUALITYRISK_S2T"
            st.session_state["compat_target_db"] = target_db.strip() or "STAGE_TEST"
            st.session_state["compat_view_db"] = view_db.strip() or view_cfg.database
            st.session_state["compat_view_schema"] = view_schema.strip() or view_cfg.schema
            st.success("Configuration updated.")
            mapping_cfg = get_mapping_config()
            view_cfg = get_view_config()
        
        # Mapping view reference and refresh button inside Builder Configuration
        st.markdown("**Mapping View**")
        st.caption(f"Source: `{mapping_cfg.label()}.SOURCE_TO_TARGET_MAPPING_VW`")
        
        col_refresh, col_hint = st.columns([1, 3])
        with col_refresh:
            if st.button("Refresh Mapping View", disabled=not conn, key="refresh_btn"):
                ok, msg = create_mapping_view(conn, mode, mapping_cfg)
                if ok:
                    run_query.clear()
                    st.success("Mapping view refreshed.")
                else:
                    st.error(f"Unable to refresh mapping view: {msg}")

    try:
        mapping_df = fetch_mapping_rows(conn, mode, mapping_cfg)
    except Exception as exc:
        st.error(
            "Mapping view is unavailable. Create test data or refresh the mapping view."
        )
        st.exception(exc)
        return

    if mapping_df.empty:
        st.warning("No source-to-target mappings found.")
        return

    domain_options = sorted(mapping_df["DOMAIN"].dropna().unique().tolist())
    database_options = sorted(mapping_df["SOURCE_DATABASE"].dropna().unique().tolist())
    schema_options = sorted(mapping_df["SOURCE_SCHEMA_NAME"].dropna().unique().tolist())
    source_table_options = sorted(mapping_df["SOURCE_TABLE_NAME"].dropna().unique().tolist())
    target_table_options = sorted(mapping_df["TARGET_TABLE_NAME"].dropna().unique().tolist())

    # Create Legacy Projection View section (now on top)
    st.markdown("### Create Legacy Projection View")
    
    # Filters for Create View section
    col1, col2 = st.columns(2)
    with col1:
        selected_domains = st.multiselect(
            "Filter by Domain",
            options=domain_options,
            default=domain_options,
            key="create_view_domain_filter",
        )
    with col2:
        selected_databases = st.multiselect(
            "Filter by Source Database",
            options=database_options,
            default=["SCANDS_Prod"] if "SCANDS_Prod" in database_options else [],
            key="create_view_database_filter",
        )
    
    col3, col4 = st.columns(2)
    with col3:
        selected_schemas = st.multiselect(
            "Filter by Source Schema",
            options=schema_options,
            default=["dbo"] if "dbo" in schema_options else [],
            key="create_view_schema_filter",
        )
    with col4:
        pass  # Placeholder for alignment
    
    # Apply filters to mapping data
    filtered_for_create = mapping_df[
        mapping_df["DOMAIN"].isin(selected_domains)
        & mapping_df["SOURCE_DATABASE"].isin(selected_databases)
        & mapping_df["SOURCE_SCHEMA_NAME"].isin(selected_schemas)
    ]
    
    # Source or Target table selection
    selection_mode = st.radio(
        "Select by:",
        options=["Source Table", "Target Table"],
        horizontal=True,
        key="table_selection_mode",
    )
    
    if selection_mode == "Source Table":
        available_tables = sorted(filtered_for_create["SOURCE_TABLE_NAME"].dropna().unique().tolist())
        if available_tables:
            selected_table = st.selectbox(
                "Select Source Table",
                options=available_tables,
                key="create_view_table_select",
            )
            if selected_table:
                table_rows = filtered_for_create[filtered_for_create["SOURCE_TABLE_NAME"] == selected_table]
        else:
            st.info("No source tables available for selected filters.")
            table_rows = None
    else:  # Target Table
        available_tables = sorted(filtered_for_create["TARGET_TABLE_NAME"].dropna().unique().tolist())
        if available_tables:
            selected_table = st.selectbox(
                "Select Target Table",
                options=available_tables,
                key="create_view_target_table_select",
            )
            if selected_table:
                table_rows = filtered_for_create[filtered_for_create["TARGET_TABLE_NAME"] == selected_table]
        else:
            st.info("No target tables available for selected filters.")
            table_rows = None
    
    # Environment selection
    source_env = st.selectbox(
        "Source Environment",
        options=["DEV", "TEST", "UAT", "PROD"],
        index=1,  # Default to TEST (index 1)
        key="create_view_env_select",
    )
    
    # Uppercase columns checkbox
    uppercase_cols = st.checkbox("Uppercase column names", value=True, key="uppercase_columns_checkbox")
    
    if table_rows is not None and not table_rows.empty:
        # For target table selection, we need to get all source mappings for that target
        if selection_mode == "Target Table":
            # Show all source columns that map to this target table
            pass
        
        # Generate the CREATE VIEW SQL
        create_view_sql = build_create_view_sql(table_rows, source_env, view_cfg, mode, uppercase_columns=uppercase_cols)
        
        # Display view query in expandable section
        with st.expander("📋 View Query", expanded=False):
            st.code(create_view_sql, language="sql")
        
        # Run query button
        if st.button("▶️ Run Query", key="run_query_btn", disabled=not conn, type="primary"):
            try:
                ok, msg = execute_ddl_dml(conn, mode, create_view_sql)
                if ok:
                    st.success(f"View created successfully!")
                    
                    # Run validation queries
                    target_table = table_rows["TARGET_TABLE_NAME"].iloc[0]
                    target_db = view_cfg.database
                    target_schema = view_cfg.schema
                    view_fqn = f"{quote_ident(target_db)}.{quote_ident(target_schema)}.{quote_ident(target_table)}"
                    
                    # Execute COUNT(*) query
                    count_query = f"SELECT COUNT(*) AS RECORD_COUNT FROM {view_fqn};"
                    try:
                        count_result = run_query(conn, count_query, mode)
                        if not count_result.empty:
                            count = count_result.iloc[0][0]
                            st.info(f"✅ Record Count: {count:,}")
                    except Exception as e:
                        st.warning(f"Could not get record count: {e}")
                    
                    # Execute MAX(LOAD_DATETIME) query against the SOURCE table (not the created view)
                    # Build source fully-qualified stage reference similar to the view builder
                    source_table_src = table_rows["SOURCE_TABLE_NAME"].iloc[0]
                    source_schema_src = table_rows["SOURCE_SCHEMA_NAME"].iloc[0]
                    source_db_src = table_rows["SOURCE_DATABASE"].iloc[0] if "SOURCE_DATABASE" in table_rows.columns else "UNKNOWN"
                    source_stage_db = f"STAGE_{source_env.upper()}"
                    source_stage_schema = f"{source_db_src.upper()}_{source_schema_src.upper()}"
                    source_table_upper = source_table_src.upper()
                    source_fqn = f"{quote_ident(source_stage_db)}.{quote_ident(source_stage_schema)}.{quote_ident(source_table_upper)}"
                    max_datetime_query = f"SELECT MAX(LOAD_DATETIME) AS LATEST_LOAD FROM {source_fqn};"
                    try:
                        max_result = run_query(conn, max_datetime_query, mode)
                        if not max_result.empty:
                            max_dt = max_result.iloc[0][0]
                            st.info(f"📅 Latest Load (source): {max_dt}")
                    except Exception as e:
                        st.warning(f"Could not get latest load datetime from source: {e}")
                else:
                    st.error(f"Failed to create view: {msg}")
            except Exception as exc:
                st.error(f"Error creating view: {str(exc)}")

    st.markdown("---")

    # Display filtered mapping using the same filters from Create View section
    filtered = mapping_df[
        mapping_df["DOMAIN"].isin(selected_domains)
        & mapping_df["SOURCE_SCHEMA_NAME"].isin(selected_schemas)
    ]
    
    # Further filter by selected table if one was chosen
    if selection_mode == "Source Table" and 'selected_table' in locals() and selected_table:
        filtered = filtered[filtered["SOURCE_TABLE_NAME"] == selected_table]
    elif selection_mode == "Target Table" and 'selected_table' in locals() and selected_table:
        filtered = filtered[filtered["TARGET_TABLE_NAME"] == selected_table]

    # Show mapping view reference
    st.markdown("### Source-to-Target Mapping")
    st.dataframe(filtered, width="stretch", hide_index=True)


main()
