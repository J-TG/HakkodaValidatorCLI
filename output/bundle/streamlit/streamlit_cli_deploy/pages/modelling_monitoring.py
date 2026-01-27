"""Model validation monitoring dashboard."""

from __future__ import annotations

import re
import json
from datetime import datetime
from typing import Any

import pandas as pd
import streamlit as st

from common.layout import init_page, get_runtime_mode
from common.db import get_snowflake_table_prefix, run_query


def _get_table_name(base: str, mode: str) -> str:
    if mode in {"snowflake_local", "snowflake_deployed"}:
        return f"{get_snowflake_table_prefix()}.{base}"
    return base


def _escape_literal(value: str | None) -> str:
    if value is None:
        return "NULL"
    safe = str(value).replace("'", "''")
    return f"'{safe}'"


def _normalize_token(value: str | None) -> str:
    return re.sub(r"[^A-Z0-9_]+", "_", (value or "").upper() or "TARGET")


def _build_task_name(table_name: str, environment: str) -> str:
    table_token = _normalize_token(table_name)
    env_token = _normalize_token(environment)
    return f"DATA_VAULT_TEMP.MIGRATION_COPILOT.VALIDATION_{table_token}_{env_token}_TASK_RUNBATCHSQL"


def _build_fqn(database: str, schema: str, table: str) -> str:
    def q(identifier: str) -> str:
        safe = (identifier or "").replace("\"", "\"\"")
        return f'"{safe}"'

    return f"{q(database)}.{q(schema)}.{q(table)}"


def _load_scheduled_summary(conn: Any, mode: str) -> pd.DataFrame:
    table_name = _get_table_name("VALIDATION_MODEL_RUNS", mode)
    sql = f"""
WITH scheduled AS (
    SELECT RUN_ENVIRONMENT, TABLE_NAME, SOURCE_DB, SOURCE_SCHEMA,
           TARGET_DB, TARGET_SCHEMA, SCHEDULE_START_DATE, SCHEDULE_END_DATE,
           RUN_AT, RUN_STATUS
    FROM {table_name}
    WHERE COALESCE(SCHEDULE_FLAG, FALSE)
), ranked AS (
    SELECT scheduled.*, ROW_NUMBER() OVER (
        PARTITION BY RUN_ENVIRONMENT, TABLE_NAME
        ORDER BY RUN_AT DESC
    ) AS rn
    FROM scheduled
)
SELECT RUN_ENVIRONMENT, TABLE_NAME, SOURCE_DB, SOURCE_SCHEMA,
       TARGET_DB, TARGET_SCHEMA, SCHEDULE_START_DATE, SCHEDULE_END_DATE,
       RUN_AT AS LAST_RUN_AT, RUN_STATUS AS LAST_STATUS
FROM ranked
WHERE rn = 1
ORDER BY RUN_ENVIRONMENT, TABLE_NAME;
"""
    try:
        df = run_query(conn, sql, mode)
    except Exception as exc:  # pragma: no cover - shown in UI
        st.error("Unable to load scheduled run summary.")
        st.exception(exc)
        return pd.DataFrame()

    if df.empty:
        return df

    for column in ["SCHEDULE_START_DATE", "SCHEDULE_END_DATE", "LAST_RUN_AT"]:
        df[column] = pd.to_datetime(df[column])

    today = pd.Timestamp(datetime.utcnow().date())

    def _calc_days(row: pd.Series) -> int | None:
        start = row["SCHEDULE_START_DATE"]
        if pd.isna(start):
            return None
        end = row["SCHEDULE_END_DATE"]
        effective_end = end if not pd.isna(end) else today
        return max(int((effective_end - start).days), 0)

    df["DAYS_ACTIVE"] = df.apply(_calc_days, axis=1)

    df["TASK_NAME"] = df.apply(
        lambda row: _build_task_name(row["TABLE_NAME"], row["RUN_ENVIRONMENT"] or "DEV"),
        axis=1,
    )
    df["SOURCE_FQN"] = df.apply(
        lambda row: _build_fqn(row["SOURCE_DB"], row["SOURCE_SCHEMA"], row["TABLE_NAME"]),
        axis=1,
    )
    df["TARGET_FQN"] = df.apply(
        lambda row: _build_fqn(row["TARGET_DB"], row["TARGET_SCHEMA"], row["TABLE_NAME"]),
        axis=1,
    )
    status_series = df["LAST_STATUS"].str.upper().fillna("UNKNOWN")
    df["STATUS_DISPLAY"] = status_series.replace(
        {
            "SUCCESS": "PASS",
            "SUCCEEDED": "PASS",
            "FAILED": "FAIL",
            "FAILURE": "FAIL",
        }
    )
    df["LAST_RUN_AT"] = df["LAST_RUN_AT"].dt.strftime("%Y-%m-%d %H:%M:%S")

    rename_map = {
        "TASK_NAME": "Task",
        "STATUS_DISPLAY": "Status",
        "DAYS_ACTIVE": "Days Active",
        "SOURCE_FQN": "Source Table",
        "TARGET_FQN": "Compatibility View",
        "LAST_RUN_AT": "Last Run",
    }
    display_df = df[list(rename_map.keys())].rename(columns=rename_map)

    return display_df


def _load_target_index(conn: Any, mode: str) -> pd.DataFrame:
    table_name = _get_table_name("VALIDATION_MODEL_RUNS", mode)
    sql = f"""
SELECT DISTINCT TARGET_DB, TARGET_SCHEMA, TABLE_NAME
FROM {table_name}
ORDER BY TARGET_DB, TARGET_SCHEMA, TABLE_NAME;
"""
    try:
        return run_query(conn, sql, mode)
    except Exception:
        return pd.DataFrame()


def _load_runs_for_target(
    conn: Any,
    mode: str,
    target_db: str,
    target_schema: str,
    table_name: str,
    limit: int = 50,
) -> pd.DataFrame:
    table = _get_table_name("VALIDATION_MODEL_RUNS", mode)
    sql = f"""
SELECT RUN_ID, RUN_AT, RUN_TYPE, SCHEDULE_FLAG, RUN_ENVIRONMENT, RUN_STATUS,
       SOURCE_DB, SOURCE_SCHEMA, TARGET_DB, TARGET_SCHEMA, TABLE_NAME,
       SAMPLE_SIZE, JOIN_KEY, SOURCE_HIGH_WATERMARK, TARGET_HIGH_WATERMARK,
       GENERATED_JSON, NOTES
FROM {table}
WHERE UPPER(TARGET_DB) = UPPER({_escape_literal(target_db)})
  AND UPPER(TARGET_SCHEMA) = UPPER({_escape_literal(target_schema)})
  AND UPPER(TABLE_NAME) = UPPER({_escape_literal(table_name)})
ORDER BY RUN_AT DESC
LIMIT {limit};
"""
    try:
        df = run_query(conn, sql, mode)
    except Exception as exc:  # pragma: no cover - UI feedback only
        st.error("Unable to load run history for the selected compatibility view.")
        st.exception(exc)
        return pd.DataFrame()
    if not df.empty and "RUN_AT" in df:
        df["RUN_AT"] = pd.to_datetime(df["RUN_AT"])
    return df


def _load_step_results(conn: Any, mode: str, run_id: str) -> pd.DataFrame:
    table = _get_table_name("VALIDATION_MODEL_RESULTS", mode)
    sql = f"""
SELECT STEP_NAME, STATUS, ROW_COUNT, RESULT_DATA
FROM {table}
WHERE RUN_ID = {_escape_literal(run_id)}
ORDER BY STEP_NAME;
"""
    try:
        return run_query(conn, sql, mode)
    except Exception:
        return pd.DataFrame()


def _load_run_rollup(conn: Any, mode: str) -> pd.DataFrame:
    table = _get_table_name("VALIDATION_MODEL_RUNS", mode)
    sql = f"""
SELECT
    RUN_ENVIRONMENT,
    TABLE_NAME,
    MIN(RUN_AT) AS FIRST_RUN_AT,
    MAX(RUN_AT) AS LAST_RUN_AT,
    COUNT(*) AS TOTAL_RUNS,
    SUM(CASE WHEN UPPER(COALESCE(RUN_STATUS, '')) IN ('SUCCESS','SUCCEEDED','PASS') THEN 1 ELSE 0 END) AS SUCCESS_RUNS,
    SUM(CASE WHEN UPPER(COALESCE(RUN_STATUS, '')) IN ('FAIL','FAILED','ERROR') THEN 1 ELSE 0 END) AS FAILURE_RUNS
FROM {table}
WHERE COALESCE(SCHEDULE_FLAG, FALSE)
GROUP BY RUN_ENVIRONMENT, TABLE_NAME
ORDER BY RUN_ENVIRONMENT, TABLE_NAME;
"""
    try:
        df = run_query(conn, sql, mode)
    except Exception:
        return pd.DataFrame()
    if not df.empty:
        for col in ["FIRST_RUN_AT", "LAST_RUN_AT"]:
            if col in df:
                df[col] = pd.to_datetime(df[col])
    return df


def _format_result_payload(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    if isinstance(value, (dict, list)):
        try:
            return json.dumps(value, indent=2)
        except Exception:
            return str(value)
    if isinstance(value, str):
        try:
            return json.dumps(json.loads(value), indent=2)
        except Exception:
            return value
    return str(value)


def _render_summary_section(conn: Any, mode: str) -> None:
    st.subheader("Scheduled Job Overview")
    summary_df = _load_scheduled_summary(conn, mode)
    if summary_df.empty:
        st.info("No scheduled modelling validation commands found yet.")
        return

    total = len(summary_df)
    pass_count = (summary_df["Status"] == "PASS").sum()
    error_count = (summary_df["Status"].isin(["ERROR", "FAIL"])).sum()
    status_cols = st.columns(3)
    status_cols[0].metric("Scheduled Jobs", total)
    status_cols[1].metric("Passing", pass_count)
    status_cols[2].metric("Errors", error_count)

    st.dataframe(summary_df, width="stretch", hide_index=True)


def _render_detail_section(conn: Any, mode: str) -> None:
    st.subheader("Detailed Run Explorer")
    target_index = _load_target_index(conn, mode)
    if target_index.empty:
        st.info("Run history is empty. Execute a validation run to populate telemetry.")
        return

    target_index = target_index.copy()
    target_index["TARGET_DB"] = target_index["TARGET_DB"].fillna("")
    target_index["TARGET_SCHEMA"] = target_index["TARGET_SCHEMA"].fillna("")

    db_options = sorted(target_index["TARGET_DB"].unique().tolist())
    default_db = "DATA_VAULT_TEMP" if "DATA_VAULT_TEMP" in db_options else db_options[0]
    selected_db = st.selectbox(
        "Compatibility View Database",
        options=db_options,
        index=db_options.index(default_db),
    )

    schema_options = sorted(
        target_index[target_index["TARGET_DB"].str.upper() == selected_db.upper()]["TARGET_SCHEMA"].unique().tolist()
    )
    default_schema = "INFO_MART" if "INFO_MART" in schema_options else schema_options[0]
    selected_schema = st.selectbox(
        "Compatibility View Schema",
        options=schema_options,
        index=schema_options.index(default_schema),
    )

    table_options = sorted(
        target_index[
            (target_index["TARGET_DB"].str.upper() == selected_db.upper())
            & (target_index["TARGET_SCHEMA"].str.upper() == selected_schema.upper())
        ]["TABLE_NAME"].dropna().unique().tolist()
    )
    if not table_options:
        st.warning("No compatibility views found for the selected database/schema.")
        return

    selected_table = st.selectbox("Compatibility View Name", options=table_options)
    run_history = _load_runs_for_target(conn, mode, selected_db, selected_schema, selected_table)
    if run_history.empty:
        st.info("No runs captured for the selected compatibility view yet.")
        return

    display_history = run_history.copy()
    display_history["RUN_AT"] = display_history["RUN_AT"].dt.strftime("%Y-%m-%d %H:%M:%S")
    display_history.rename(
        columns={
            "RUN_AT": "Run Timestamp",
            "RUN_TYPE": "Run Type",
            "SCHEDULE_FLAG": "Scheduled",
            "RUN_ENVIRONMENT": "Environment",
            "RUN_STATUS": "Status",
        },
        inplace=True,
    )
    st.dataframe(display_history[
        ["RUN_ID", "Run Timestamp", "Run Type", "Scheduled", "Environment", "Status", "SAMPLE_SIZE", "JOIN_KEY"]
    ], width="stretch", hide_index=True)

    run_id = st.selectbox("Inspect run details", options=run_history["RUN_ID"].tolist())
    if run_id:
        results_df = _load_step_results(conn, mode, run_id)
        if results_df.empty:
            st.warning("No persisted step results found for this run.")
            return
        formatted = results_df.copy()
        formatted["RESULT_DATA"] = formatted["RESULT_DATA"].apply(_format_result_payload)
        st.markdown("#### Step Results")
        st.dataframe(formatted, width="stretch", hide_index=True)


def _render_run_rollup(conn: Any, mode: str) -> None:
    st.subheader("Run Rollup (Scheduled)")
    rollup_df = _load_run_rollup(conn, mode)
    if rollup_df.empty:
        st.info("No scheduled run history found yet.")
        return

    display = rollup_df.copy()
    display["FIRST_RUN_AT"] = display["FIRST_RUN_AT"].dt.strftime("%Y-%m-%d %H:%M:%S")
    display["LAST_RUN_AT"] = display["LAST_RUN_AT"].dt.strftime("%Y-%m-%d %H:%M:%S")
    display.rename(
        columns={
            "RUN_ENVIRONMENT": "Environment",
            "TABLE_NAME": "Compatibility View",
            "FIRST_RUN_AT": "First Run",
            "LAST_RUN_AT": "Last Run",
            "TOTAL_RUNS": "Runs",
            "SUCCESS_RUNS": "Successes",
            "FAILURE_RUNS": "Failures",
        },
        inplace=True,
    )

    st.dataframe(
        display,
        width="stretch",
        hide_index=True,
        column_config={
            "Environment": st.column_config.TextColumn(width="small"),
            "Compatibility View": st.column_config.TextColumn(width="medium"),
            "First Run": st.column_config.TextColumn(width="medium"),
            "Last Run": st.column_config.TextColumn(width="medium"),
            "Runs": st.column_config.NumberColumn(width="small"),
            "Successes": st.column_config.NumberColumn(width="small"),
            "Failures": st.column_config.NumberColumn(width="small"),
        },
    )


def main() -> None:
    conn = init_page()
    mode = get_runtime_mode()

    st.title("🧠 Model Validation Monitoring")
    st.markdown(
        "Track scheduled modelling validation commands, inspect their Snowflake task bindings, and drill into the latest run outputs."
    )

    if conn is None:
        st.error("Database connection unavailable.")
        st.stop()

    _render_summary_section(conn, mode)
    st.markdown("---")
    _render_detail_section(conn, mode)
    st.markdown("---")
    _render_run_rollup(conn, mode)


main()
