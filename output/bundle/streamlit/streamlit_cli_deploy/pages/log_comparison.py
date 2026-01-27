"""
Log Comparison page

Compare source vs target row counts across environments for selected tables over the last 7 days.
"""

import pandas as pd
import streamlit as st
import altair as alt

from common.layout import init_page, get_runtime_mode
from common.db import run_query, get_snowflake_table_prefix


def _table_ref(name: str, mode: str) -> str:
    if mode == "duckdb":
        return name
    prefix = get_snowflake_table_prefix()
    return f"{prefix}.{name}" if prefix else name


def _fetch_df(conn, mode: str, sql: str) -> pd.DataFrame:
    try:
        return run_query(conn, sql, mode)
    except Exception as exc:  # pragma: no cover - UI surface only
        st.warning(f"Query failed: {exc}")
        return pd.DataFrame()


def _render_summary(df: pd.DataFrame) -> None:
    if df.empty:
        st.info("No data available for the selected source.")
        return

    summary = (
        df.groupby("ENVIRONMENT")
        .agg(
            SOURCE_ROWS_7D=("SOURCE_ROW_COUNT", "sum"),
            TARGET_ROWS_7D=("TARGET_ROW_COUNT", "sum"),
            LAST_REFRESH=("LAST_REFRESH_TIME", "max"),
            LAST_RUN_DATE=("RUN_DATE", "max"),
        )
        .reset_index()
    )
    summary["ROW_DELTA"] = summary["TARGET_ROWS_7D"] - summary["SOURCE_ROWS_7D"]
    st.markdown("### Environment Summary (Last 7 Days)")
    st.dataframe(summary, width="stretch", hide_index=True)

    chart = (
        alt.Chart(df)
        .mark_line(point=True)
        .encode(
            x=alt.X("RUN_DATE:T", title="Run Date"),
            y=alt.Y("TARGET_ROW_COUNT:Q", title="Rows (target)"),
            color=alt.Color("ENVIRONMENT:N", title="Environment"),
            tooltip=["ENVIRONMENT", "RUN_DATE", "TARGET_ROW_COUNT", "SOURCE_ROW_COUNT", "ROW_DELTA"],
        )
        .properties(title="Rows ingested per day (last 7d)", width="container", height=280)
    )
    st.altair_chart(chart, theme=None)


def main():
    conn = init_page()
    mode = get_runtime_mode()

    st.title("📊 Log Comparison")
    st.caption("Compare per-table ingestion across environments for the last 7 days.")

    if conn is None:
        st.info("Connect to DuckDB or Snowflake to view metrics.")
        return

    comparison_view = _table_ref("ELT_LOAD_COMPARISON_7D_VW", mode)

    base_sql = f"""
SELECT ENVIRONMENT, SOURCE_NAME, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, TABLE_TYPE, RUN_DATE, SOURCE_ROW_COUNT, TARGET_ROW_COUNT, LAST_REFRESH_TIME
FROM {comparison_view}
ORDER BY TABLE_NAME, RUN_DATE DESC
"""

    df_all = _fetch_df(conn, mode, base_sql)

    if df_all.empty:
        st.info("No log comparison data available. Run test data setup to seed ELT_LOAD_COMPARISON.")
        return

    # Multi-level filters
    db_options = ["All"] + sorted(df_all["DATABASE_NAME"].dropna().unique())
    selected_db = st.selectbox("Database", options=db_options, index=0)

    filtered_df = df_all.copy()
    if selected_db != "All":
        filtered_df = filtered_df[filtered_df["DATABASE_NAME"] == selected_db]

    schema_options = ["All"] + sorted(filtered_df["SCHEMA_NAME"].dropna().unique())
    selected_schema = st.selectbox("Schema", options=schema_options, index=0)
    if selected_schema != "All":
        filtered_df = filtered_df[filtered_df["SCHEMA_NAME"] == selected_schema]

    table_options = sorted(filtered_df["TABLE_NAME"].unique())
    selected = st.selectbox("Table", options=table_options, index=None, placeholder="Choose a table to compare")

    if not selected:
        st.stop()

    filtered = filtered_df[filtered_df["TABLE_NAME"] == selected].copy()
    cutoff = pd.Timestamp.now(tz="UTC").normalize().tz_localize(None) - pd.Timedelta(days=7)
    filtered["RUN_DATE"] = pd.to_datetime(filtered["RUN_DATE"], utc=True).dt.tz_convert("UTC").dt.tz_localize(None)
    filtered = filtered[filtered["RUN_DATE"] >= cutoff]
    filtered["ROW_DELTA"] = filtered["TARGET_ROW_COUNT"] - filtered["SOURCE_ROW_COUNT"]

    _render_summary(filtered)


main()
