"""Validation → Ingestion metrics dashboard."""

from typing import Tuple

import pandas as pd
import streamlit as st

from common.layout import init_page, get_runtime_mode
from common.db import run_query, get_snowflake_table_prefix

KEY_COLUMNS = ["CHECK_TYPE", "OBJECT_NAME", "METRIC_NAME"]
MATCH_STATUSES = ["match", "mismatch", "source_only", "target_only"]


def get_validation_tables(mode: str) -> Tuple[str, str]:
    if mode in {"snowflake_local", "snowflake_deployed"}:
        prefix = get_snowflake_table_prefix()
        return (
            f"{prefix}.VALIDATION_METRICS_SOURCE",
            f"{prefix}.VALIDATION_METRICS_TARGET",
        )
    return "VALIDATION_METRICS_SOURCE", "VALIDATION_METRICS_TARGET"


def _rename_non_keys(df: pd.DataFrame, suffix: str) -> pd.DataFrame:
    rename_map = {
        col: f"{col}_{suffix}"
        for col in df.columns
        if col not in KEY_COLUMNS
    }
    return df.rename(columns=rename_map)


def _normalize_text(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def _classify_match(row: pd.Series) -> str:
    source_val = row.get("METRIC_VALUE_SOURCE")
    target_val = row.get("METRIC_VALUE_TARGET")
    if pd.isna(source_val) and pd.isna(target_val):
        return "match"
    if pd.isna(source_val):
        return "target_only"
    if pd.isna(target_val):
        return "source_only"
    source_sev = _normalize_text(row.get("SEVERITY_SOURCE"))
    target_sev = _normalize_text(row.get("SEVERITY_TARGET"))
    if _normalize_text(source_val) == _normalize_text(target_val) and source_sev.upper() == target_sev.upper():
        return "match"
    return "mismatch"


def _split_object_name(obj: str) -> Tuple[str, str, str]:
    if not obj:
        return ("", "", "")
    cleaned = obj.replace("`", "").replace("'", "").replace("\"", "")
    parts = [p for p in cleaned.split(".") if p]
    if len(parts) >= 3:
        return (parts[0], parts[1], parts[2])
    return ("", "", "")


def main() -> None:
    conn = init_page()
    mode = get_runtime_mode()

    st.title("✅ Ingestion Validation")
    st.markdown(
        "Join SQL Server source statistics to Snowflake target runs using the paired "
        "`VALIDATION_METRICS_SOURCE` and `VALIDATION_METRICS_TARGET` tables generated in Test Data Setup."
    )

    source_table, target_table = get_validation_tables(mode)
    st.caption(f"Source metrics: `{source_table}` — Target metrics: `{target_table}`")

    if conn is None:
        st.error("No active database connection.")
        return

    with st.spinner("Loading ingestion metrics..."):
        try:
            source_df = run_query(conn, f"SELECT * FROM {source_table};", mode)
            target_df = run_query(conn, f"SELECT * FROM {target_table};", mode)
        except Exception as exc:  # pragma: no cover - surfaced via UI
            st.warning(
                "Unable to read validation metrics. Run the Test Data Setup to create both tables and try again."
            )
            st.exception(exc)
            return

    if source_df.empty and target_df.empty:
        st.info(
            "No metrics recorded yet. Seed both tables via Test Data Setup or ingest real validations."
        )
        return

    source_df = _rename_non_keys(source_df, "SOURCE")
    target_df = _rename_non_keys(target_df, "TARGET")
    comparison = pd.merge(source_df, target_df, on=KEY_COLUMNS, how="outer")
    comparison["match_status"] = comparison.apply(_classify_match, axis=1)

    # Derive database/schema/table from OBJECT_NAME to support filtering
    db_schema_table = comparison["OBJECT_NAME"].fillna("").apply(_split_object_name)
    comparison["DATABASE_NAME"], comparison["SCHEMA_NAME"], comparison["TABLE_NAME"] = zip(*db_schema_table)

    severity_values = sorted(
        {
            str(sev)
            for sev in pd.concat(
                [
                    comparison["SEVERITY_SOURCE"],
                    comparison["SEVERITY_TARGET"],
                ]
            ).dropna().unique()
        }
    )
    check_types = sorted(comparison["CHECK_TYPE"].dropna().unique().tolist())

    st.markdown("### Filters")
    db_options = ["All"] + sorted({v for v in comparison["DATABASE_NAME"].dropna() if str(v).strip()})
    selected_db = st.selectbox("Database", db_options, index=0)

    filtered_comp = comparison.copy()
    if selected_db != "All":
        filtered_comp = filtered_comp[filtered_comp["DATABASE_NAME"] == selected_db]

    schema_options = ["All"] + sorted({v for v in filtered_comp["SCHEMA_NAME"].dropna() if str(v).strip()})
    selected_schema = st.selectbox("Schema", schema_options, index=0)
    if selected_schema != "All":
        filtered_comp = filtered_comp[filtered_comp["SCHEMA_NAME"] == selected_schema]

    table_options = ["All"] + sorted({v for v in filtered_comp["TABLE_NAME"].dropna() if str(v).strip()})
    selected_table = st.selectbox("Table", table_options, index=0)
    if selected_table != "All":
        filtered_comp = filtered_comp[filtered_comp["TABLE_NAME"] == selected_table]

    selected_checks = st.multiselect(
        "Check Types",
        check_types,
        default=check_types,
    )
    selected_severities = st.multiselect(
        "Severities (any side)",
        severity_values,
        default=severity_values,
    )
    selected_status = st.multiselect(
        "Match Status",
        MATCH_STATUSES,
        default=MATCH_STATUSES,
        help="Highlight rows with matching metrics, mismatches, or entries present on only one side.",
    )

    mask = filtered_comp["CHECK_TYPE"].isin(selected_checks)
    if selected_severities:
        source_sev_mask = filtered_comp["SEVERITY_SOURCE"].fillna("").astype(str).isin(selected_severities)
        target_sev_mask = filtered_comp["SEVERITY_TARGET"].fillna("").astype(str).isin(selected_severities)
        mask &= source_sev_mask | target_sev_mask
    mask &= filtered_comp["match_status"].isin(selected_status)

    filtered = filtered_comp[mask]

    st.markdown("### Metric Summary")
    if filtered.empty:
        st.warning("No rows match the current filters.")
    else:
        summary = (
            filtered.groupby(["CHECK_TYPE", "match_status"], dropna=False)
            .size()
            .reset_index(name="metric_count")
            .sort_values(["CHECK_TYPE", "match_status"], ignore_index=True)
        )
        st.dataframe(summary, width="stretch", hide_index=True)

    st.markdown("### Detailed Metrics")
    detail_columns = [
        "CHECK_TYPE",
        "OBJECT_NAME",
        "METRIC_NAME",
        "METRIC_VALUE_SOURCE",
        "SEVERITY_SOURCE",
        "METRIC_VALUE_TARGET",
        "SEVERITY_TARGET",
        "match_status",
        "NOTES_SOURCE",
        "NOTES_TARGET",
    ]
    present_columns = [col for col in detail_columns if col in filtered.columns]
    displayed = filtered[present_columns].fillna("—")
    st.dataframe(
        displayed,
        width="stretch",
        hide_index=True,
        column_config={
            "CHECK_TYPE": st.column_config.TextColumn("Check Type", width="small"),
            "OBJECT_NAME": st.column_config.TextColumn("Object", width="large"),
            "METRIC_NAME": st.column_config.TextColumn("Metric", width="large"),
            "METRIC_VALUE_SOURCE": st.column_config.TextColumn("Source Value", width="small"),
            "SEVERITY_SOURCE": st.column_config.TextColumn("Source Severity", width="small"),
            "METRIC_VALUE_TARGET": st.column_config.TextColumn("Target Value", width="small"),
            "SEVERITY_TARGET": st.column_config.TextColumn("Target Severity", width="small"),
            "match_status": st.column_config.TextColumn("Match Status", width="small"),
            "NOTES_SOURCE": st.column_config.TextColumn("Source Notes", width="large"),
            "NOTES_TARGET": st.column_config.TextColumn("Target Notes", width="large"),
        },
    )

    st.markdown("### Next Steps")
    st.caption(
        "Use the comparison to pinpoint ingestion regressions before promoting changes to production."
    )


main()
