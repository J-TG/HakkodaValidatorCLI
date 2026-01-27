"""
Ingestion Monitoring page

Job run metrics view driven by ELT_JOB_RUN* artifacts from test data or warehouse.
"""

import streamlit as st
import pandas as pd
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


def _render_metrics(df_runs: pd.DataFrame, df_rollup: pd.DataFrame) -> None:
    total_runs = len(df_runs)
    successes = len(df_runs[df_runs["STATUS"].str.upper() == "SUCCEDED"]) if not df_runs.empty else 0
    failures = len(df_runs[df_runs["STATUS"].str.upper().isin(["FAILURE", "FAILED"])]) if not df_runs.empty else 0
    success_rate = (successes / total_runs * 100) if total_runs else 0

    total_rows = int(df_runs["ROWS_PROCESSED"].sum()) if not df_runs.empty else 0
    latest_end = df_runs["END_TIME"].max() if not df_runs.empty else None

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Runs", f"{total_runs}")
    col2.metric("Success Rate", f"{success_rate:.1f}%")
    col3.metric("Rows Processed", f"{total_rows:,}")
    col4.metric("Last End", str(latest_end) if latest_end is not None else "—")

    st.markdown("### Rollup by Trigger & Status")
    st.dataframe(df_rollup, width="stretch", hide_index=True)


def _render_recent(df_runs: pd.DataFrame) -> None:
    st.markdown("### Recent Runs")
    st.dataframe(df_runs, width="stretch", hide_index=True)


def _render_load_metrics(
    df_summary: pd.DataFrame, df_last: pd.DataFrame, df_errors: pd.DataFrame
) -> None:
    st.markdown("## Load Log Metrics")

    if df_summary.empty and df_last.empty and df_errors.empty:
        st.info("No load log data available. Run test data setup to seed ELT_LOAD_LOG.")
        return

    total_runs = int(df_summary["RUNS"].sum()) if not df_summary.empty else 0
    total_rows = int(df_summary["TOTAL_ROWS"].sum()) if not df_summary.empty else 0
    total_errors = int(df_summary["TOTAL_ERRORS"].sum()) if not df_summary.empty else 0
    last_end = df_summary["LAST_END_TIME"].max() if not df_summary.empty else None

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Loads", f"{total_runs}")
    col2.metric("Rows Loaded", f"{total_rows:,}")
    col3.metric("Total Errors", f"{total_errors}")
    col4.metric("Last Load End", str(last_end) if last_end is not None else "—")

    if not df_summary.empty:
        st.markdown("### Summary by Load")
        st.dataframe(df_summary, width="stretch", hide_index=True)

    if not df_last.empty:
        st.markdown("### Last Run per Load")
        st.dataframe(df_last, width="stretch", hide_index=True)

    if not df_errors.empty:
        st.markdown("### Error Counts by Day")
        st.dataframe(df_errors, width="stretch", hide_index=True)


def _render_error_metrics(
    df_daily: pd.DataFrame, df_top: pd.DataFrame, df_last: pd.DataFrame
) -> None:
    st.markdown("## Error Log Metrics")

    if df_daily.empty and df_top.empty and df_last.empty:
        st.info("No error log data available. Run test data setup to seed ELT_JOB_RUN_ERROR.")
        return

    total_errors = int(df_daily["TOTAL_ERRORS"].sum()) if not df_daily.empty else 0
    unique_stages = df_daily["SOURCE_STAGE"].nunique() if not df_daily.empty else 0
    last_error_ts = df_last["LAST_ERROR_TIMESTAMP"].max() if not df_last.empty else None

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Errors", f"{total_errors}")
    col2.metric("Stages Impacted", f"{unique_stages}")
    col3.metric("Last Error", str(last_error_ts) if last_error_ts is not None else "—")

    if not df_daily.empty:
        st.markdown("### Daily Errors by Stage")
        st.dataframe(df_daily, width="stretch", hide_index=True)

    if not df_top.empty:
        st.markdown("### Top Error Messages (per Stage)")
        st.dataframe(df_top, width="stretch", hide_index=True)

    if not df_last.empty:
        st.markdown("### Last Error per Stage")
        st.dataframe(df_last, width="stretch", hide_index=True)


def _render_env_summary(df_env_summary: pd.DataFrame) -> None:
    st.markdown("### Environment Summary (Last 24h)")
    if df_env_summary.empty:
        st.info("No environment summary available. Ensure ELT_ENVIRONMENT_SUMMARY_24H_VW is populated.")
        return
    st.dataframe(df_env_summary, width="stretch", hide_index=True)


def _render_health_charts(
    df_job_daily: pd.DataFrame,
    df_rollup: pd.DataFrame,
    df_load_errors: pd.DataFrame,
    df_error_daily: pd.DataFrame,
) -> None:
    st.markdown("## Ingestion Health (Key Charts)")

    charts = []

    # 1) Runs per day by status
    if not df_job_daily.empty:
        chart = (
            alt.Chart(df_job_daily)
            .mark_line(point=True)
            .encode(
                x=alt.X("RUN_DATE:T", title="Run Date"),
                y=alt.Y("RUNS:Q", title="Runs"),
                color=alt.Color("STATUS:N", title="Status"),
                tooltip=["RUN_DATE:T", "STATUS:N", "RUNS:Q", "TRIGGER_SOURCE:N"],
            )
            .properties(title="Runs per Day by Status", width="container", height=260)
        )
        charts.append(chart)

    # 2) Success rate by trigger
    if not df_rollup.empty:
        grouped = df_rollup.copy()
        grouped["STATUS"] = grouped["STATUS"].str.upper()
        success = grouped[grouped["STATUS"] == "SUCCEDED"].groupby("TRIGGER_SOURCE")["RUNS"].sum()
        total = grouped.groupby("TRIGGER_SOURCE")["RUNS"].sum()
        rate_df = (
            pd.DataFrame({"TRIGGER_SOURCE": total.index, "SUCCESS_RATE": (success / total * 100).fillna(0).values})
            .sort_values("SUCCESS_RATE", ascending=False)
        )
        chart = (
            alt.Chart(rate_df)
            .mark_bar()
            .encode(
                x=alt.X("TRIGGER_SOURCE:N", title="Trigger"),
                y=alt.Y("SUCCESS_RATE:Q", title="Success Rate (%)"),
                tooltip=["TRIGGER_SOURCE:N", alt.Tooltip("SUCCESS_RATE:Q", format=".1f")],
            )
            .properties(title="Success Rate by Trigger", width="container", height=240)
        )
        charts.append(chart)

    # 3) Rows processed by trigger (sum across statuses)
    if not df_rollup.empty:
        rows_df = (
            df_rollup.groupby("TRIGGER_SOURCE")["TOTAL_ROWS_PROCESSED"].sum().reset_index().sort_values("TOTAL_ROWS_PROCESSED", ascending=False)
        )
        chart = (
            alt.Chart(rows_df)
            .mark_bar()
            .encode(
                x=alt.X("TRIGGER_SOURCE:N", title="Trigger"),
                y=alt.Y("TOTAL_ROWS_PROCESSED:Q", title="Rows Processed"),
                tooltip=["TRIGGER_SOURCE:N", "TOTAL_ROWS_PROCESSED:Q"],
            )
            .properties(title="Rows Processed by Trigger", width="container", height=240)
        )
        charts.append(chart)

    # 4) Load errors per day by load
    if not df_load_errors.empty:
        chart = (
            alt.Chart(df_load_errors)
            .mark_bar()
            .encode(
                x=alt.X("RUN_DATE:T", title="Run Date"),
                y=alt.Y("TOTAL_ERRORS:Q", title="Errors"),
                color=alt.Color("LOAD_NAME:N", title="Load"),
                tooltip=["RUN_DATE:T", "LOAD_NAME:N", "TOTAL_ERRORS:Q", "RUNS_WITH_ERRORS:Q"],
            )
            .properties(title="Load Errors per Day", width="container", height=260)
        )
        charts.append(chart)

    # 5) Error counts by source stage
    if not df_error_daily.empty:
        stage_df = df_error_daily.groupby("SOURCE_STAGE")["TOTAL_ERRORS"].sum().reset_index().sort_values("TOTAL_ERRORS", ascending=False)
        chart = (
            alt.Chart(stage_df)
            .mark_bar()
            .encode(
                x=alt.X("SOURCE_STAGE:N", title="Source Stage"),
                y=alt.Y("TOTAL_ERRORS:Q", title="Total Errors"),
                tooltip=["SOURCE_STAGE:N", "TOTAL_ERRORS:Q"],
            )
            .properties(title="Errors by Source Stage", width="container", height=240)
        )
        charts.append(chart)

    if not charts:
        st.info("No data available to render ingestion health charts.")
        return

    # Lay out charts in two columns where possible
    for i in range(0, len(charts), 2):
        cols = st.columns(2)
        cols[0].altair_chart(charts[i], theme=None)
        if i + 1 < len(charts):
            cols[1].altair_chart(charts[i + 1], theme=None)


def main():
    conn = init_page()
    mode = get_runtime_mode()

    st.title("📥 Ingestion Monitoring")
    st.caption("Job run metrics from ELT_JOB_RUN and rollups.")

    env_options = ["DEV", "TEST", "UAT", "PROD"]
    selected_envs = st.multiselect(
        "Environments",
        options=env_options,
        default=env_options,
        help="Choose which environments to include in the summary view.",
    )
    show_combined = st.checkbox(
        "Show combined summary for selected environments",
        value=True,
        help="When checked, the summary aggregates all selected environments. Tabs below always show one environment at a time.",
    )

    if conn is None:
        st.info("Connect to DuckDB or Snowflake to view metrics.")
        return

    job_table = _table_ref("ELT_JOB_RUN", mode)
    metrics_view = _table_ref("ELT_JOB_RUN_METRICS_VW", mode)
    job_daily_view = _table_ref("ELT_JOB_RUN_DAILY_VW", mode)
    env_summary_view = _table_ref("ELT_ENVIRONMENT_SUMMARY_24H_VW", mode)
    load_summary_view = _table_ref("ELT_LOAD_LOG_SUMMARY_VW", mode)
    load_last_view = _table_ref("ELT_LOAD_LOG_LAST_RUN_VW", mode)
    load_errors_view = _table_ref("ELT_LOAD_LOG_ERROR_DAILY_VW", mode)
    error_daily_view = _table_ref("ELT_ERROR_LOG_DAILY_VW", mode)
    error_top_view = _table_ref("ELT_ERROR_LOG_TOP_MESSAGES_VW", mode)
    error_last_view = _table_ref("ELT_ERROR_LOG_LAST_VW", mode)

    runs_sql = f"""
SELECT RUN_ID, START_TIME, END_TIME, STATUS, ROWS_PROCESSED, ERROR_COUNT, EXECUTION_TIME, TRIGGER_SOURCE
FROM {job_table}
ORDER BY START_TIME DESC
LIMIT 50
"""

    rollup_sql = f"SELECT * FROM {metrics_view} ORDER BY TRIGGER_SOURCE, STATUS"
    job_daily_sql = f"SELECT * FROM {job_daily_view} ORDER BY RUN_DATE"
    env_summary_sql = f"SELECT * FROM {env_summary_view}"
    load_summary_sql = f"SELECT * FROM {load_summary_view}"
    load_last_sql = f"SELECT * FROM {load_last_view}"
    load_errors_sql = f"SELECT * FROM {load_errors_view}"
    error_daily_sql = f"SELECT * FROM {error_daily_view}"
    error_top_sql = f"SELECT * FROM {error_top_view}"
    error_last_sql = f"SELECT * FROM {error_last_view}"

    df_runs = _fetch_df(conn, mode, runs_sql)
    df_rollup = _fetch_df(conn, mode, rollup_sql)
    df_job_daily = _fetch_df(conn, mode, job_daily_sql)
    df_env_summary = _fetch_df(conn, mode, env_summary_sql)
    df_load_summary = _fetch_df(conn, mode, load_summary_sql)
    df_load_last = _fetch_df(conn, mode, load_last_sql)
    df_load_errors = _fetch_df(conn, mode, load_errors_sql)
    df_error_daily = _fetch_df(conn, mode, error_daily_sql)
    df_error_top = _fetch_df(conn, mode, error_top_sql)
    df_error_last = _fetch_df(conn, mode, error_last_sql)

    if df_runs.empty:
        st.info("No job run data available. Run test data setup to seed ELT_JOB_RUN.")
        return

    def _filter_env(df: pd.DataFrame, envs: list[str]) -> pd.DataFrame:
        if df.empty or "ENVIRONMENT" not in df.columns:
            return df
        return df[df["ENVIRONMENT"].str.upper().isin(envs)]

    # Combined summary for selected environments (if available)
    if show_combined:
        st.markdown("### Summary (Selected Environments)")
        _render_env_summary(_filter_env(df_env_summary, selected_envs))
        st.divider()
        _render_health_charts(
            _filter_env(df_job_daily, selected_envs),
            _filter_env(df_rollup, selected_envs),
            _filter_env(df_load_errors, selected_envs),
            _filter_env(df_error_daily, selected_envs),
        )
        st.divider()
        _render_metrics(_filter_env(df_runs, selected_envs), _filter_env(df_rollup, selected_envs))
        st.divider()
        _render_recent(_filter_env(df_runs, selected_envs))
        st.divider()
        _render_load_metrics(
            _filter_env(df_load_summary, selected_envs),
            _filter_env(df_load_last, selected_envs),
            _filter_env(df_load_errors, selected_envs),
        )
        st.divider()
        _render_error_metrics(
            _filter_env(df_error_daily, selected_envs),
            _filter_env(df_error_top, selected_envs),
            _filter_env(df_error_last, selected_envs),
        )

    # Environment-specific tabs
    tabs = st.tabs(env_options)
    for env, tab in zip(env_options, tabs):
        with tab:
            st.markdown(f"#### {env} Environment")
            env_filter = [env]
            _render_env_summary(_filter_env(df_env_summary, env_filter))
            st.divider()
            _render_health_charts(
                _filter_env(df_job_daily, env_filter),
                _filter_env(df_rollup, env_filter),
                _filter_env(df_load_errors, env_filter),
                _filter_env(df_error_daily, env_filter),
            )
            st.divider()
            _render_metrics(
                _filter_env(df_runs, env_filter), _filter_env(df_rollup, env_filter)
            )
            st.divider()
            _render_recent(_filter_env(df_runs, env_filter))
            st.divider()
            _render_load_metrics(
                _filter_env(df_load_summary, env_filter),
                _filter_env(df_load_last, env_filter),
                _filter_env(df_load_errors, env_filter),
            )
            st.divider()
            _render_error_metrics(
                _filter_env(df_error_daily, env_filter),
                _filter_env(df_error_top, env_filter),
                _filter_env(df_error_last, env_filter),
            )


main()
