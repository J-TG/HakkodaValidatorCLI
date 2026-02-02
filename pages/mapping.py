"""
Source-to-Target Mapping Viewer

Displays and analyzes source-to-target column mappings across multiple domains
(Finance, Production, Quality & Risk), organized by table and columns.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

import pandas as pd
import streamlit as st

from common.layout import init_page, get_runtime_mode
from common.db import (
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
    st.session_state.setdefault("compat_mapping_db", "DATA_VAULT_DEV")
    st.session_state.setdefault("compat_mapping_schema", "INFO_MART")
    st.session_state.setdefault("compat_finance_schema", "INFO_MART")
    st.session_state.setdefault("compat_prod_schema", "INFO_MART")
    st.session_state.setdefault("compat_quality_schema", "INFO_MART")
    st.session_state.setdefault("compat_target_db", "STAGE_TEST")
    st.session_state.setdefault("compat_view_db", "DATA_VAULT_TEMP")
    st.session_state.setdefault("compat_view_schema", "INFO_MART")


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
        else:
            sql = f"DROP VIEW IF EXISTS {view_cfg.fq_table(table, mode)};"
        ok, msg = execute_ddl_dml(conn, mode, sql)
        outcomes.append((table, ok, "Completed" if ok else msg))
    run_query.clear()
    return outcomes


def load_review_corrections(conn, mode) -> pd.DataFrame:
    """Load correction records from MAPPING_REVIEW_CORRECTIONS table."""
    table_fqn = "MAPPING_REVIEW_CORRECTIONS" if mode == "duckdb" else "DATA_VAULT_TEMP.MIGRATION.MAPPING_REVIEW_CORRECTIONS"
    query = f"SELECT * FROM {table_fqn} ORDER BY LAST_MODIFIED_DATE DESC"
    try:
        return run_query(conn, query, mode)
    except Exception:
        return pd.DataFrame()


def save_review_correction(conn, mode, mapping_id: str, object_type: str, source_table: str, 
                          source_column: str, original_target: str, suggested_target: str,
                          updated_target: str, needs_review: bool, status: str) -> tuple[bool, str]:
    """Save a mapping correction record."""
    table_fqn = "MAPPING_REVIEW_CORRECTIONS" if mode == "duckdb" else "DATA_VAULT_TEMP.MIGRATION.MAPPING_REVIEW_CORRECTIONS"
    
    # Use MERGE or INSERT based on whether record exists
    upsert_sql = f"""
    MERGE INTO {table_fqn} t
    USING (SELECT '{mapping_id}' as MAPPING_ID) s
    ON t.MAPPING_ID = s.MAPPING_ID
    WHEN MATCHED THEN UPDATE SET 
        OBJECT_TYPE = '{object_type}',
        SOURCE_TABLE_NAME = '{source_table}',
        SOURCE_COLUMN_NAME = '{source_column}',
        ORIGINAL_TARGET_NAME = '{original_target}',
        SUGGESTED_TARGET_NAME = '{suggested_target}',
        UPDATED_TARGET_NAME = '{updated_target}',
        NEEDS_REVIEW = {needs_review},
        STATUS = '{status}',
        LAST_MODIFIED_DATE = CURRENT_TIMESTAMP
    WHEN NOT MATCHED THEN INSERT 
        (MAPPING_ID, OBJECT_TYPE, SOURCE_TABLE_NAME, SOURCE_COLUMN_NAME, ORIGINAL_TARGET_NAME, 
         SUGGESTED_TARGET_NAME, UPDATED_TARGET_NAME, NEEDS_REVIEW, STATUS, CREATED_DATE, LAST_MODIFIED_DATE)
    VALUES 
        ('{mapping_id}', '{object_type}', '{source_table}', '{source_column}', '{original_target}',
         '{suggested_target}', '{updated_target}', {needs_review}, '{status}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
    """
    return execute_ddl_dml(conn, mode, upsert_sql)


def load_mapping_rules(conn, mode) -> pd.DataFrame:
    """Load mapping rules from MAPPING_RULES table."""
    table_fqn = "MAPPING_RULES" if mode == "duckdb" else "DATA_VAULT_TEMP.MIGRATION.MAPPING_RULES"
    query = f"SELECT * FROM {table_fqn} ORDER BY SORT_ORDER, CREATED_DATE"
    try:
        return run_query(conn, query, mode)
    except Exception:
        return pd.DataFrame()


def save_mapping_rule(conn, mode, rule_id: str, rule_name: str, rule_type: str, 
                      source_pattern: str, target_pattern: str, description: str,
                      example_source: str, example_target: str, applies_to: str) -> tuple[bool, str]:
    """Save a new mapping rule."""
    table_fqn = "MAPPING_RULES" if mode == "duckdb" else "DATA_VAULT_TEMP.MIGRATION.MAPPING_RULES"
    
    insert_sql = f"""
    INSERT INTO {table_fqn} 
        (RULE_ID, RULE_NAME, RULE_TYPE, SOURCE_PATTERN, TARGET_PATTERN, DESCRIPTION,
         EXAMPLE_SOURCE, EXAMPLE_TARGET, IS_ACTIVE, APPLIES_TO, CREATED_DATE, LAST_MODIFIED_DATE, CREATED_BY)
    VALUES 
        ('{rule_id}', '{rule_name}', '{rule_type}', '{source_pattern}', '{target_pattern}', '{description}',
         '{example_source}', '{example_target}', TRUE, '{applies_to}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'user');
    """
    return execute_ddl_dml(conn, mode, insert_sql)


def toggle_mapping_rule(conn, mode, rule_id: str, is_active: bool) -> tuple[bool, str]:
    """Mark a mapping rule as inactive (soft delete)."""
    table_fqn = "MAPPING_RULES" if mode == "duckdb" else "DATA_VAULT_TEMP.MIGRATION.MAPPING_RULES"
    
    update_sql = f"""
    UPDATE {table_fqn}
    SET IS_ACTIVE = {is_active}, LAST_MODIFIED_DATE = CURRENT_TIMESTAMP
    WHERE RULE_ID = '{rule_id}';
    """
    return execute_ddl_dml(conn, mode, update_sql)


def main() -> None:
    conn = init_page()
    mode = get_runtime_mode()
    ensure_config_defaults()
    mapping_cfg = get_mapping_config()
    view_cfg = get_view_config()
    snowflake_enabled = mode in ("snowflake_local", "snowflake_deployed")

    st.title("🗺️ Source-to-Target Mapping")
    st.markdown(
        "Explore and analyze column mappings from legacy source systems to Snowflake targets."
    )

    # Refresh and display mapping
    try:
        if st.button("🔄 Refresh Mappings", type="secondary"):
            ok, msg = create_mapping_view(conn, mode, mapping_cfg)
            if ok:
                st.success("Mapping view refreshed.")
                st.cache_data.clear()
                run_query.clear()
            else:
                st.error(f"Failed to refresh: {msg}")

        mapping_df = fetch_mapping_rows(conn, mode, mapping_cfg)

        if mapping_df.empty:
            st.warning("No mapping data found. Try refreshing.")
            return

        # Filters
        col1, col2 = st.columns(2)
        with col1:
            domain_options = sorted(mapping_df["DOMAIN"].dropna().unique().tolist())
            selected_domains = st.multiselect(
                "Domains",
                options=domain_options,
                default=domain_options,
            )

        with col2:
            schema_options = sorted(mapping_df["SOURCE_SCHEMA_NAME"].dropna().unique().tolist())
            selected_schemas = st.multiselect(
                "Source Schemas",
                options=schema_options,
                default=schema_options,
            )

        filtered = mapping_df[
            mapping_df["DOMAIN"].isin(selected_domains)
            & mapping_df["SOURCE_SCHEMA_NAME"].isin(selected_schemas)
        ]

        # Mapping Rules Section
        with st.expander("📋 Mapping Rules & Conventions", expanded=False):
            st.markdown("**Naming and transformation rules applied to source-to-target mappings**")
            
            rules_df = load_mapping_rules(conn, mode)
            
            if not rules_df.empty:
                # Display active rules
                active_rules = rules_df[rules_df["IS_ACTIVE"] == True] if "IS_ACTIVE" in rules_df.columns else rules_df
                inactive_rules = rules_df[rules_df["IS_ACTIVE"] == False] if "IS_ACTIVE" in rules_df.columns else pd.DataFrame()
                
                if not active_rules.empty:
                    st.markdown("#### Active Rules")
                    col1, col2, col3, col4, col5, col6 = st.columns([1, 1.5, 1.5, 2, 1.5, 0.8])
                    with col1:
                        st.write("**Type**")
                    with col2:
                        st.write("**Applies To**")
                    with col3:
                        st.write("**Rule Name**")
                    with col4:
                        st.write("**Example**")
                    with col5:
                        st.write("**Description**")
                    with col6:
                        st.write("**Action**")
                    
                    for idx, rule in active_rules.iterrows():
                        col1, col2, col3, col4, col5, col6 = st.columns([1, 1.5, 1.5, 2, 1.5, 0.8])
                        with col1:
                            st.caption(rule.get("RULE_TYPE", ""))
                        with col2:
                            st.caption(rule.get("APPLIES_TO", ""))
                        with col3:
                            st.caption(rule.get("RULE_NAME", ""))
                        with col4:
                            example = f"{rule.get('EXAMPLE_SOURCE', '')} → {rule.get('EXAMPLE_TARGET', '')}"
                            st.caption(example)
                        with col5:
                            st.caption(rule.get("DESCRIPTION", ""))
                        with col6:
                            if st.button("Remove", key=f"remove_rule_{idx}", use_container_width=True):
                                ok, msg = toggle_mapping_rule(conn, mode, rule.get("RULE_ID", ""), False)
                                if ok:
                                    st.success("Rule removed")
                                    run_query.clear()
                                    st.rerun()
                                else:
                                    st.error(f"Error: {msg}")
                
                if not inactive_rules.empty:
                    st.markdown("#### Removed Rules (Struck Through)")
                    col1, col2, col3, col4, col5, col6 = st.columns([1, 1.5, 1.5, 2, 1.5, 0.8])
                    with col1:
                        st.write("**Type**")
                    with col2:
                        st.write("**Applies To**")
                    with col3:
                        st.write("**Rule Name**")
                    with col4:
                        st.write("**Example**")
                    with col5:
                        st.write("**Description**")
                    with col6:
                        st.write("**Action**")
                    
                    for idx, rule in inactive_rules.iterrows():
                        col1, col2, col3, col4, col5, col6 = st.columns([1, 1.5, 1.5, 2, 1.5, 0.8])
                        with col1:
                            st.caption(f"~~{rule.get('RULE_TYPE', '')}~~")
                        with col2:
                            st.caption(f"~~{rule.get('APPLIES_TO', '')}~~")
                        with col3:
                            st.caption(f"~~{rule.get('RULE_NAME', '')}~~")
                        with col4:
                            example = f"~~{rule.get('EXAMPLE_SOURCE', '')} → {rule.get('EXAMPLE_TARGET', '')}~~"
                            st.caption(example)
                        with col5:
                            st.caption(f"~~{rule.get('DESCRIPTION', '')}~~")
                        with col6:
                            if st.button("Restore", key=f"restore_rule_{idx}", use_container_width=True):
                                ok, msg = toggle_mapping_rule(conn, mode, rule.get("RULE_ID", ""), True)
                                if ok:
                                    st.success("Rule restored")
                                    run_query.clear()
                                    st.rerun()
                                else:
                                    st.error(f"Error: {msg}")
                
                st.divider()
            
            # Add new rule form
            st.markdown("#### Add New Rule")
            with st.form("add_rule_form"):
                col1, col2 = st.columns(2)
                with col1:
                    new_rule_name = st.text_input("Rule Name", placeholder="e.g., Primary Key Suffix")
                    new_rule_type = st.selectbox("Rule Type", ["PREFIX_MAPPING", "CASE_CONVERSION", "SUFFIX_MAPPING", "PATTERN_REPLACEMENT"])
                    new_source_pattern = st.text_input("Source Pattern", placeholder="e.g., camelCase or _PK")
                with col2:
                    new_applies_to = st.selectbox("Applies To", ["TABLE", "COLUMN", "BOTH"])
                    new_target_pattern = st.text_input("Target Pattern", placeholder="e.g., UPPER_SNAKE_CASE")
                    new_description = st.text_area("Description", placeholder="Explain this rule", height=60)
                
                col1, col2 = st.columns(2)
                with col1:
                    new_example_source = st.text_input("Example Source", placeholder="e.g., customerID")
                with col2:
                    new_example_target = st.text_input("Example Target", placeholder="e.g., CUSTOMER_ID")
                
                add_submitted = st.form_submit_button("➕ Add Rule", type="primary")
                
                if add_submitted:
                    if all([new_rule_name, new_rule_type, new_source_pattern, new_target_pattern, new_example_source, new_example_target]):
                        import uuid
                        rule_id = f"RULE_{str(uuid.uuid4())[:8].upper()}"
                        ok, msg = save_mapping_rule(
                            conn, mode, rule_id, new_rule_name, new_rule_type,
                            new_source_pattern, new_target_pattern, new_description,
                            new_example_source, new_example_target, new_applies_to
                        )
                        if ok:
                            st.success(f"✅ Rule '{new_rule_name}' added successfully")
                            run_query.clear()
                            st.rerun()
                        else:
                            st.error(f"Error: {msg}")
                    else:
                        st.error("Please fill in all required fields")

        # Create tabs for Table-level, Column-level, and Needs Review views
        tabs = st.tabs(["📊 Table-level Mappings", "📋 Column-level Mapping", "🔍 Needs Review"])

        # TABLE-LEVEL TAB
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

            table_keys = table_display["SOURCE_TABLE"].fillna("").tolist()
            selected_table = st.selectbox(
                "Select Source Table",
                options=table_keys,
                index=-1 if not table_keys else 0,
                help="Pick a legacy source table to view its mappings.",
                key="table_level_table",
            )

            # Display filtered table data
            if selected_table:
                filtered_display = table_display[table_display["SOURCE_TABLE"] == selected_table]
                st.dataframe(filtered_display, width="stretch", hide_index=True, height=600)
                
                # Add Needs Review checkbox
                needs_review = st.checkbox(
                    "Mark this table for review",
                    value=False,
                    key=f"needs_review_table_{selected_table}",
                    help="Check to add this table to the Needs Review section"
                )
                
                if needs_review:
                    if st.button("Add to Needs Review", type="secondary", key=f"add_review_btn_{selected_table}"):
                        # Get the mapping rows for this table
                        table_rows = filtered[filtered["SOURCE_TABLE_NAME"] == selected_table]
                        if not table_rows.empty:
                            for _, row in table_rows.iterrows():
                                correction_record = {
                                    "MAPPING_ID": str(__import__("uuid").uuid4()),
                                    "OBJECT_TYPE": "TABLE",
                                    "SOURCE_TABLE_NAME": row.get("SOURCE_TABLE_NAME", ""),
                                    "SOURCE_COLUMN_NAME": row.get("SOURCE_COLUMN_NAME", ""),
                                    "ORIGINAL_TARGET_NAME": row.get("TARGET_TABLE_NAME", ""),
                                    "SUGGESTED_TARGET_NAME": row.get("TARGET_TABLE_NAME", ""),
                                    "UPDATED_TARGET_NAME": "",
                                    "NEEDS_REVIEW": True,
                                    "CREATED_AT": __import__("datetime").datetime.now().isoformat(),
                                    "CREATED_BY": "user",
                                    "STATUS": "PENDING",
                                    "TEAM": "",
                                    "NOTES": ""
                                }
                                # Save to database
                                merge_sql = f"""
                                MERGE INTO {quote_ident(view_cfg.database)}.{quote_ident(view_cfg.schema)}.MAPPING_REVIEW_CORRECTIONS AS t
                                USING (SELECT '{correction_record["MAPPING_ID"]}' AS MAPPING_ID) AS s
                                ON t.MAPPING_ID = s.MAPPING_ID
                                WHEN NOT MATCHED THEN INSERT (
                                    MAPPING_ID, OBJECT_TYPE, SOURCE_TABLE_NAME, SOURCE_COLUMN_NAME,
                                    ORIGINAL_TARGET_NAME, SUGGESTED_TARGET_NAME, UPDATED_TARGET_NAME,
                                    NEEDS_REVIEW, CREATED_AT, CREATED_BY, STATUS, TEAM, NOTES
                                ) VALUES (
                                    '{correction_record["MAPPING_ID"]}', '{correction_record["OBJECT_TYPE"]}',
                                    '{correction_record["SOURCE_TABLE_NAME"]}', '{correction_record["SOURCE_COLUMN_NAME"]}',
                                    '{correction_record["ORIGINAL_TARGET_NAME"]}', '{correction_record["SUGGESTED_TARGET_NAME"]}',
                                    '', TRUE, CURRENT_TIMESTAMP, 'user', 'PENDING', '', ''
                                );
                                """
                                try:
                                    execute_ddl_dml(conn, mode, merge_sql)
                                except Exception as e:
                                    st.warning(f"Could not save to review table (may not exist): {e}")
                            st.success(f"Added {selected_table} to Needs Review!")
                            st.rerun()
            else:
                st.dataframe(table_display, width="stretch", hide_index=True, height=600)

            create_disabled = not selected_table or not snowflake_enabled
            if st.button("Create Views From Selected Table", type="primary", disabled=create_disabled, key="create_table_views"):
                if selected_table:
                    results = perform_view_action("create", [selected_table], filtered, conn, mode, view_cfg)
                    for table, ok, message in results:
                        (st.success if ok else st.error)(f"{table}: {message}")

        # COLUMN-LEVEL TAB
        with tabs[1]:
            table_filter_options = sorted(filtered["SOURCE_TABLE_NAME"].dropna().unique().tolist())
            selected_table = st.selectbox(
                "Source Table",
                options=table_filter_options,
                index=-1 if not table_filter_options else 0,
                help="Choose the legacy source table to generate compatibility views for.",
                key="column_level_table",
            )

            st.markdown("### Full Column Mapping Preview")
            if selected_table:
                filtered_cols = filtered[filtered["SOURCE_TABLE_NAME"] == selected_table]
                st.dataframe(filtered_cols, width="stretch", hide_index=True, height=600)
            else:
                st.dataframe(filtered, width="stretch", hide_index=True, height=600)

            if selected_table:
                st.markdown("### View Creation Preview")
                rows = filtered[filtered["SOURCE_TABLE_NAME"] == selected_table]
                render_sql_preview(rows, view_cfg, mode)
            else:
                st.info("Select a source table to preview create statements.")

            action_cols = st.columns(2)
            create_disabled = not selected_table or not snowflake_enabled
            drop_disabled = not selected_table or not snowflake_enabled

            with action_cols[0]:
                if st.button("Create Selected View", type="primary", disabled=create_disabled, key="create_column_views"):
                    if selected_table:
                        results = perform_view_action("create", [selected_table], filtered, conn, mode, view_cfg)
                        for table, ok, message in results:
                            (st.success if ok else st.error)(f"{table}: {message}")

            with action_cols[1]:
                if st.button("Drop Selected View", disabled=drop_disabled, key="drop_column_views"):
                    results = perform_view_action("drop", selected_tables, filtered, conn, mode, view_cfg)
                    for table, ok, message in results:
                        (st.success if ok else st.error)(f"{table}: {message}")

        # NEEDS REVIEW TAB
        with tabs[2]:
            st.markdown("### Mappings Flagged for Review")
            st.markdown("Items marked for review appear here. You can correct the target names and update the status.")
            
            # Load review corrections from database
            corrections_df = load_review_corrections(conn, mode)
            
            if corrections_df.empty:
                st.info("No mappings currently flagged for review.")
            else:
                # Display corrections in orange highlight if needs_review is true
                st.markdown("#### Current Review Items")
                
                # Create editable form for corrections
                with st.form("review_corrections_form"):
                    st.markdown("**Make corrections below and click Save to update status**")
                    
                    review_items = corrections_df[corrections_df["NEEDS_REVIEW"] == True].copy() if "NEEDS_REVIEW" in corrections_df.columns else corrections_df.copy()
                    
                    if not review_items.empty:
                        col1, col2, col3, col4, col5, col6 = st.columns(6)
                        
                        with col1:
                            st.write("**Object**")
                        with col2:
                            st.write("**Source Table**")
                        with col3:
                            st.write("**Current Target**")
                        with col4:
                            st.write("**Suggested**")
                        with col5:
                            st.write("**Updated Target**")
                        with col6:
                            st.write("**Needs Review**")
                        
                        updated_rows = []
                        for idx, row in review_items.iterrows():
                            col1, col2, col3, col4, col5, col6 = st.columns(6)
                            
                            with col1:
                                obj_type = st.selectbox(
                                    "Object Type",
                                    options=["TABLE", "COLUMN"],
                                    index=0 if row.get("OBJECT_TYPE", "TABLE") == "TABLE" else 1,
                                    key=f"obj_type_{idx}",
                                    label_visibility="collapsed"
                                )
                            
                            with col2:
                                source_tbl = st.text_input(
                                    "Source Table",
                                    value=row.get("SOURCE_TABLE_NAME", ""),
                                    key=f"source_tbl_{idx}",
                                    disabled=True,
                                    label_visibility="collapsed"
                                )
                            
                            with col3:
                                current_target = st.text_input(
                                    "Current Target",
                                    value=row.get("ORIGINAL_TARGET_NAME", ""),
                                    key=f"current_target_{idx}",
                                    disabled=True,
                                    label_visibility="collapsed"
                                )
                            
                            with col4:
                                suggested_target = st.text_input(
                                    "Suggested",
                                    value=row.get("SUGGESTED_TARGET_NAME", ""),
                                    key=f"suggested_{idx}",
                                    disabled=True,
                                    label_visibility="collapsed"
                                )
                            
                            with col5:
                                updated_target = st.text_input(
                                    "Updated Target",
                                    value=row.get("UPDATED_TARGET_NAME", "") or row.get("SUGGESTED_TARGET_NAME", ""),
                                    placeholder="Enter corrected name",
                                    key=f"updated_target_{idx}",
                                    label_visibility="collapsed"
                                )
                            
                            with col6:
                                needs_review = st.checkbox(
                                    "Needs Review",
                                    value=row.get("NEEDS_REVIEW", True),
                                    key=f"needs_review_{idx}",
                                    label_visibility="collapsed"
                                )
                            
                            updated_rows.append({
                                "mapping_id": row.get("MAPPING_ID", f"auto_{idx}"),
                                "object_type": obj_type,
                                "source_table": source_tbl,
                                "source_column": row.get("SOURCE_COLUMN_NAME", ""),
                                "original_target": current_target,
                                "suggested_target": suggested_target,
                                "updated_target": updated_target,
                                "needs_review": needs_review,
                                "status": "MODIFIED" if updated_target != row.get("SUGGESTED_TARGET_NAME", "") else "NEEDS_REVIEW"
                            })
                    
                    save_cols = st.columns([1, 4])
                    with save_cols[0]:
                        submitted = st.form_submit_button("💾 Save Corrections", type="primary")
                    
                    if submitted and updated_rows:
                        for row_data in updated_rows:
                            ok, msg = save_review_correction(
                                conn, mode,
                                mapping_id=row_data["mapping_id"],
                                object_type=row_data["object_type"],
                                source_table=row_data["source_table"],
                                source_column=row_data["source_column"],
                                original_target=row_data["original_target"],
                                suggested_target=row_data["suggested_target"],
                                updated_target=row_data["updated_target"],
                                needs_review=row_data["needs_review"],
                                status=row_data["status"]
                            )
                            if ok:
                                st.success(f"✅ {row_data['source_table']} ({row_data['object_type']}): {row_data['status']}")
                            else:
                                st.error(f"❌ {row_data['source_table']}: {msg}")
                        run_query.clear()
                    
                    if not review_items.empty:
                        st.divider()
                        st.markdown("#### All Review Records")
                        st.dataframe(corrections_df, width="stretch", hide_index=True, height=400)
                    else:
                        st.info("No corrections currently marked for review.")

    except Exception as e:
        st.error(f"An error occurred: {str(e)}")
        import traceback
        st.error(traceback.format_exc())


main()
