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
    st.session_state.setdefault("compat_mapping_db", "DATA_VAULT_DEV")
    st.session_state.setdefault("compat_mapping_schema", "INFO_MART")
    st.session_state.setdefault("compat_finance_schema", "INFO_MART")
    st.session_state.setdefault("compat_prod_schema", "INFO_MART")
    st.session_state.setdefault("compat_quality_schema", "INFO_MART")
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
        st.session_state.get("compat_mapping_db", "STAGE_TEST").strip() or "STAGE_TEST",
        st.session_state.get("compat_mapping_schema", "SCANDS_FINANCE").strip() or "SCANDS_FINANCE",
    )


def get_domain_schemas() -> Dict[str, str]:
    return {
        "FINANCE": (st.session_state.get("compat_finance_schema") or "SCANDS_FINANCE").strip(),
        "PROD": (st.session_state.get("compat_prod_schema") or "SCANDS_PROD").strip(),
        "QUALITYRISK": (st.session_state.get("compat_quality_schema") or "SCANDS_QUALITYRISK").strip(),
    }


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
    domain_schemas = get_domain_schemas()
    db = mapping_cfg.database
    view_fqn = mapping_cfg.fq_table("SOURCE_TO_TARGET_MAPPING_VW", mode)
    finance = f'{quote_ident(db)}.{quote_ident(domain_schemas["FINANCE"])}.{quote_ident("SCANDS_FINANCE_S2T")}' if mode != "duckdb" else quote_ident("SCANDS_FINANCE_S2T")
    prod = f'{quote_ident(db)}.{quote_ident(domain_schemas["PROD"])}.{quote_ident("SCANDS_PROD_S2T")}' if mode != "duckdb" else quote_ident("SCANDS_PROD_S2T")
    quality = f'{quote_ident(db)}.{quote_ident(domain_schemas["QUALITYRISK"])}.{quote_ident("SCANDS_QUALITYRISK_S2T")}' if mode != "duckdb" else quote_ident("SCANDS_QUALITYRISK_S2T")
    select_columns = (
        "SOURCE_SCHEMA_NAME, SOURCE_TABLE_NAME, SOURCE_COLUMN_NAME, ORDINAL_POSITION, "
        "SOURCE_DATA_TYPE, MAX_LENGTH, PRECISION, SCALE, IS_NULLABLE, "
        "TARGET_TABLE_NAME, TARGET_COLUMN_NAME, TARGET_DATA_TYPE"
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
        f"SOURCE_DATA_TYPE, TARGET_TABLE_NAME, TARGET_COLUMN_NAME, TARGET_DATA_TYPE "
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

    with st.expander("Builder Configuration", expanded=True):
        with st.form("compat_config_form"):
            col1, col2 = st.columns(2)
            with col1:
                mapping_db = st.text_input("Mapping Database", value=mapping_cfg.database)
                finance_schema = st.text_input("Finance Schema", value=st.session_state.get("compat_finance_schema", "SCANDS_FINANCE"))
                prod_schema = st.text_input("Prod Schema", value=st.session_state.get("compat_prod_schema", "SCANDS_PROD"))
                quality_schema = st.text_input("Quality Schema", value=st.session_state.get("compat_quality_schema", "SCANDS_QUALITYRISK"))
            with col2:
                env_options = get_environment_databases()
                env_list = list(env_options.keys())
                env_values = list(env_options.values())
                current_target = st.session_state.get("compat_target_db", "STAGE_TEST")
                try:
                    default_index = env_values.index(current_target)
                except ValueError:
                    default_index = 0
                selected_env = st.selectbox(
                    "Target Environment",
                    options=env_list,
                    index=default_index,
                    key="target_env_select",
                )
                target_db = env_options[selected_env]
                view_db = st.text_input("View Database", value=view_cfg.database)
                view_schema = st.text_input("View Schema", value=view_cfg.schema)
            submitted = st.form_submit_button("Save Configuration", type="secondary")
        if submitted:
            st.session_state["compat_mapping_db"] = mapping_db.strip() or mapping_cfg.database
            st.session_state["compat_mapping_schema"] = finance_schema.strip() or "SCANDS_FINANCE"
            st.session_state["compat_finance_schema"] = finance_schema.strip() or "SCANDS_FINANCE"
            st.session_state["compat_prod_schema"] = prod_schema.strip() or "SCANDS_PROD"
            st.session_state["compat_quality_schema"] = quality_schema.strip() or "SCANDS_QUALITYRISK"
            st.session_state["compat_target_db"] = target_db.strip() or "STAGE_TEST"
            st.session_state["compat_view_db"] = view_db.strip() or view_cfg.database
            st.session_state["compat_view_schema"] = view_schema.strip() or view_cfg.schema
            st.success("Configuration updated.")
            mapping_cfg = get_mapping_config()
            view_cfg = get_view_config()

    st.markdown(
        f"Mapping view source: `{mapping_cfg.label()}.SOURCE_TO_TARGET_MAPPING_VW`"
    )

    col_refresh, col_hint = st.columns([1, 3])
    with col_refresh:
        if st.button("Refresh Mapping View", disabled=not conn):
            ok, msg = create_mapping_view(conn, mode, mapping_cfg)
            if ok:
                run_query.clear()
                st.success("Mapping view refreshed.")
            else:
                st.error(f"Unable to refresh mapping view: {msg}")
    with col_hint:
        if not snowflake_enabled:
            st.info("Snowflake runtime is recommended for view operations.")

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
    schema_options = sorted(mapping_df["SOURCE_SCHEMA_NAME"].dropna().unique().tolist())
    table_options = sorted(mapping_df["SOURCE_TABLE_NAME"].dropna().unique().tolist())

    selected_domains = st.multiselect(
        "Filter by Domain",
        options=domain_options,
        default=domain_options,
    )
    selected_schemas = st.multiselect(
        "Filter by Source Schema",
        options=schema_options,
        default=schema_options,
    )

    filtered = mapping_df[
        mapping_df["DOMAIN"].isin(selected_domains)
        & mapping_df["SOURCE_SCHEMA_NAME"].isin(selected_schemas)
    ]

    # Mapping section with Table-level and Column-level tabs
    with st.expander("Mapping", expanded=True):
        tabs = st.tabs(["Table-level", "Column-level"])

        # Table-level: distinct mappings per table (server/database columns may be empty
        # if not present in the canonical S2T tables). The view creation still uses
        # the column-level mapping rows under the hood.
        with tabs[0]:
            domain_schemas = get_domain_schemas()
            target_db = get_target_db()

            table_df = (
                filtered[
                    ["DOMAIN", "SOURCE_SCHEMA_NAME", "SOURCE_TABLE_NAME", "TARGET_TABLE_NAME"]
                ]
                .drop_duplicates()
                .reset_index(drop=True)
            )
            # Fill columns that may not exist in test data
            table_df["SOURCE_SERVER"] = ""
            table_df["SOURCE_DATABASE"] = ""
            table_df["TARGET_DATABASE"] = target_db
            table_df["TARGET_SCHEMA"] = table_df["DOMAIN"].fillna("").apply(
                lambda d: domain_schemas.get((d or "").upper(), view_cfg.schema)
            )

            table_display = table_df.rename(
                columns={
                    "SOURCE_SCHEMA_NAME": "SOURCE_SCHEMA",
                    "SOURCE_TABLE_NAME": "SOURCE_TABLE",
                    "TARGET_TABLE_NAME": "TARGET_TABLE",
                }
            )[
                [
                    "SOURCE_SERVER",
                    "SOURCE_DATABASE",
                    "SOURCE_SCHEMA",
                    "SOURCE_TABLE",
                    "TARGET_DATABASE",
                    "TARGET_SCHEMA",
                    "TARGET_TABLE",
                ]
            ]

            st.markdown("### Table-level Mappings")
            st.dataframe(table_display, width="stretch", hide_index=True)

            table_keys = (
                table_display["SOURCE_SCHEMA"].fillna("") + "." + table_display["SOURCE_TABLE"].fillna("")
            ).tolist()
            selected_table_keys = st.multiselect(
                "Select Source Tables",
                options=table_keys,
                default=[],
                help="Pick legacy source tables to generate compatibility views for.",
            )

            create_disabled = not selected_table_keys or not snowflake_enabled
            if st.button("Create Views From Selected Tables", type="primary", disabled=create_disabled):
                selected_table_names = [k.split(".")[-1] for k in selected_table_keys]
                results = perform_view_action("create", selected_table_names, filtered, conn, mode, view_cfg)
                for table, ok, message in results:
                    (st.success if ok else st.error)(f"{table}: {message}")

        # Column-level: reuse the existing mapping preview and view creation flow
        with tabs[1]:
            table_filter_options = sorted(filtered["SOURCE_TABLE_NAME"].dropna().unique().tolist())
            selected_tables = st.multiselect(
                "Source Tables",
                options=table_filter_options,
                default=[],
                help="Choose the legacy source tables to generate compatibility views for.",
            )

            st.markdown("### Column-level Mapping Preview")
            st.dataframe(filtered, width="stretch", hide_index=True)

            if selected_tables:
                st.markdown("### View Creation Preview")
                for table in selected_tables:
                    st.markdown(f"**{table}**")
                    rows = filtered[filtered["SOURCE_TABLE_NAME"] == table]
                    render_sql_preview(rows, view_cfg, mode)
            else:
                st.info("Select at least one source table to preview create statements.")

            action_cols = st.columns(2)
            create_disabled = not selected_tables or not snowflake_enabled
            drop_disabled = not selected_tables or not snowflake_enabled

            with action_cols[0]:
                if st.button("Create Selected Views", type="primary", disabled=create_disabled):
                    results = perform_view_action("create", selected_tables, filtered, conn, mode, view_cfg)
                    for table, ok, message in results:
                        (st.success if ok else st.error)(f"{table}: {message}")

            with action_cols[1]:
                if st.button("Drop Selected Views", disabled=drop_disabled):
                    results = perform_view_action("drop", selected_tables, filtered, conn, mode, view_cfg)
                    for table, ok, message in results:
                        (st.success if ok else st.error)(f"{table}: {message}")


main()
