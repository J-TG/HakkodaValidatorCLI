"""Validation → Modelling comparison workflow."""

from __future__ import annotations

import json
import uuid
from datetime import date
import re
from typing import Any, Dict, List, Tuple

import pandas as pd
import streamlit as st

from common.layout import init_page, get_runtime_mode
from common.db import (
    run_query,
    execute_ddl_dml,
    get_snowflake_table_prefix,
)
from common.validation_queries import (
    ValidationQueryStep,
    get_modelling_validation_steps,
)


TABLE_OPTIONS = [
    "DIM_SITE_ATTRIBUTES",
    "DIM_MEMBER_MONTHS",
    "FACT_MEMBER_APPLICATION",
]
SOURCE_DB_OPTIONS = ["DATA_VAULT_TEMP", "DATA_VAULT_DEV"]
TARGET_DB_OPTIONS = ["_DATA_VAULT_DEV_CHRIS", "DATA_VAULT_DEV"]
SCHEMA_OPTIONS = ["INFO_MART", "SANDBOX"]
JOIN_KEY_OPTIONS = ["SITE_ATTR_ID", "DIM_KEY", "MEMBER_ID"]
HW_OPTIONS = ["DW_LAST_UPDATED_DATE", "SYSTEM_CREATE_DATE", "SYSTEM_UPDATE_DATE"]
SAMPLE_SIZE_OPTIONS = [100, 500, 1000, 5000]
EXCLUDE_PRESETS = {
    "Default system columns": "DIM_SITE_ATTRIBUTES_KEY,SYSTEM_VERSION,SYSTEM_CURRENT_FLAG,SYSTEM_START_DATE,SYSTEM_END_DATE,SYSTEM_CREATE_DATE,SYSTEM_UPDATE_DATE,DW_LAST_UPDATED_DATE",
    "Minimal (keys only)": "DIM_SITE_ATTRIBUTES_KEY",
    "None": "",
}
ENVIRONMENT_OPTIONS = ["DEV", "TEST", "UAT", "PROD"]


def _select_with_custom(
    container: st.delta_generator.DeltaGenerator,
    *,
    label: str,
    options: List[str],
    default: str,
    key: str,
) -> str:
    values = options + ["Custom..."]
    default_value = default if default in options else "Custom..."
    default_index = values.index(default_value)
    choice = container.selectbox(label, values, index=default_index, key=f"{key}_select")
    if choice == "Custom...":
        return container.text_input(f"Custom {label}", value=default, key=f"{key}_custom").strip()
    return choice


def _select_int_with_custom(
    container: st.delta_generator.DeltaGenerator,
    *,
    label: str,
    options: List[int],
    default: int,
    key: str,
) -> int:
    str_options = [str(opt) for opt in options] + ["Custom..."]
    default_value = str(default) if default in options else "Custom..."
    default_index = str_options.index(default_value)
    choice = container.selectbox(label, str_options, index=default_index, key=f"{key}_select")
    if choice == "Custom...":
        return int(
            container.number_input(
                f"Custom {label}",
                value=default,
                min_value=1,
                step=100,
                key=f"{key}_custom",
            )
        )
    return int(choice)


def _select_exclude_columns(
    container: st.delta_generator.DeltaGenerator,
    default_label: str,
    key: str,
) -> str:
    labels = list(EXCLUDE_PRESETS.keys()) + ["Custom..."]
    default_value = default_label if default_label in EXCLUDE_PRESETS else "Custom..."
    default_index = labels.index(default_value)
    choice = container.selectbox("Excluded Columns", labels, index=default_index, key=f"{key}_preset")
    if choice == "Custom...":
        return container.text_area(
            "Custom excluded columns (comma-separated)",
            value=EXCLUDE_PRESETS.get(default_label, ""),
            key=f"{key}_custom",
        ).strip()
    return EXCLUDE_PRESETS[choice]


def _escape_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    safe = str(value).replace("'", "''")
    return f"'{safe}'"


def _qualify_identifier(database: str, schema: str, table: str) -> str:
    return f'"{database}"."{schema}"."{table}"'


def _get_table_name(base: str, mode: str) -> str:
    if mode in {"snowflake_local", "snowflake_deployed"}:
        return f"{get_snowflake_table_prefix()}.{base}"
    return base


def _ensure_audit_tables(conn: Any, mode: str) -> None:
    run_table = _get_table_name("VALIDATION_MODEL_RUNS", mode)
    result_table = _get_table_name("VALIDATION_MODEL_RESULTS", mode)
    run_ddl = f"""
CREATE TABLE IF NOT EXISTS {run_table} (
    RUN_ID VARCHAR(16777216) NOT NULL,
    RUN_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    RUN_TYPE VARCHAR(50) DEFAULT 'manual',
    SCHEDULE_FLAG BOOLEAN DEFAULT FALSE,
    SCHEDULE_START_DATE DATE,
    SCHEDULE_END_DATE DATE,
    RUN_ENVIRONMENT VARCHAR(25),
    RUN_STATUS VARCHAR(50),
    TABLE_NAME VARCHAR(255),
    SOURCE_DB VARCHAR(255),
    SOURCE_SCHEMA VARCHAR(255),
    TARGET_DB VARCHAR(255),
    TARGET_SCHEMA VARCHAR(255),
    SAMPLE_SIZE NUMBER,
    JOIN_KEY VARCHAR(255),
    SOURCE_HIGH_WATERMARK VARCHAR(255),
    TARGET_HIGH_WATERMARK VARCHAR(255),
    EXCLUDE_COLS VARCHAR(16777216),
    GENERATED_JSON BOOLEAN DEFAULT FALSE,
    GENERATED_BY VARCHAR(255),
    NOTES VARCHAR(16777216),
    PRIMARY KEY (RUN_ID)
);
"""
    result_ddl = f"""
CREATE TABLE IF NOT EXISTS {result_table} (
    RUN_ID VARCHAR(16777216) NOT NULL,
    STEP_NAME VARCHAR(255) NOT NULL,
    STATUS VARCHAR(50),
    ROW_COUNT NUMBER,
    RESULT_DATA VARIANT,
    EXECUTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (RUN_ID, STEP_NAME)
);
"""
    for ddl in (run_ddl, result_ddl):
        ok, msg = execute_ddl_dml(conn, mode, ddl)
        if not ok:
            st.warning(f"Unable to ensure validation tables exist: {msg}")


def _build_query_context(params: Dict[str, Any]) -> Dict[str, Any]:
    source_db = params["source_database"].strip() or "DATA_VAULT_TEMP"
    target_db = params["target_database"].strip() or source_db
    source_schema = params["source_schema"].strip() or "INFO_MART"
    target_schema = params["target_schema"].strip() or source_schema
    table_name = params["table_name"].strip().upper()
    exclude_literal = params["exclude_cols"].replace("'", "''")
    return {
        "table_name": table_name,
        "source_db": f'"{source_db}"',
        "target_db": f'"{target_db}"',
        "source_schema": source_schema,
        "target_schema": target_schema,
        "source_fq": _qualify_identifier(source_db, source_schema, table_name),
        "target_fq": _qualify_identifier(target_db, target_schema, table_name),
        "sample_size": params["sample_size"],
        "join_key": params["join_key"].strip(),
        "exclude_cols": exclude_literal,
        "source_high_watermark": params["source_high_watermark"].strip(),
        "target_high_watermark": params["target_high_watermark"].strip(),
    }


def _normalize_token(value: str) -> str:
    return re.sub(r"[^A-Z0-9_]+", "_", value.upper())


def _command_table_name(table_name: str, environment: str) -> str:
    table_token = _normalize_token(table_name or "TABLE")
    env_token = _normalize_token(environment or "DEV")
    return f"DATA_VAULT_TEMP.MIGRATION_COPILOT.VALIDATION_{table_token}_{env_token}_COMMAND"


def _persist_run(
    conn: Any,
    mode: str,
    run_id: str,
    run_status: str,
    params: Dict[str, Any],
    results: List[Dict[str, Any]],
) -> None:
    run_table = _get_table_name("VALIDATION_MODEL_RUNS", mode)
    result_table = _get_table_name("VALIDATION_MODEL_RESULTS", mode)
    run_sql = f"""
INSERT INTO {run_table} (
    RUN_ID, RUN_AT, RUN_TYPE, SCHEDULE_FLAG, SCHEDULE_START_DATE, SCHEDULE_END_DATE,
    RUN_ENVIRONMENT,
    RUN_STATUS, TABLE_NAME, SOURCE_DB, SOURCE_SCHEMA, TARGET_DB, TARGET_SCHEMA,
    SAMPLE_SIZE, JOIN_KEY, SOURCE_HIGH_WATERMARK, TARGET_HIGH_WATERMARK, EXCLUDE_COLS,
    GENERATED_JSON, GENERATED_BY, NOTES
)
VALUES (
    {_escape_literal(run_id)},
    CURRENT_TIMESTAMP,
    {_escape_literal(params['run_type'])},
    {str(params['schedule_flag']).upper()},
    { _escape_literal(params['schedule_start_date']) if params['schedule_start_date'] else 'NULL' },
    { _escape_literal(params['schedule_end_date']) if params['schedule_end_date'] else 'NULL' },
    {_escape_literal(params['execution_environment'])},
    {_escape_literal(run_status)},
    {_escape_literal(params['table_name'])},
    {_escape_literal(params['source_database'])},
    {_escape_literal(params['source_schema'])},
    {_escape_literal(params['target_database'])},
    {_escape_literal(params['target_schema'])},
    {params['sample_size']},
    {_escape_literal(params['join_key'])},
    {_escape_literal(params['source_high_watermark'])},
    {_escape_literal(params['target_high_watermark'])},
    {_escape_literal(params['exclude_cols'])},
    {str(params['generate_json']).upper()},
    {_escape_literal(params['run_owner'])},
    {_escape_literal(params.get('notes'))}
);
"""
    execute_ddl_dml(conn, mode, run_sql)

    for result in results:
        payload = result.get("data")
        if payload is None:
            payload_json = json.dumps({"error": result.get("error", "Unknown error")})
        else:
            payload_json = json.dumps(payload)
        payload_escaped = payload_json.replace("'", "''")
        insert_step = f"""
INSERT INTO {result_table} (RUN_ID, STEP_NAME, STATUS, ROW_COUNT, RESULT_DATA, EXECUTED_AT)
VALUES (
    {_escape_literal(run_id)},
    {_escape_literal(result['step'])},
    {_escape_literal(result['status'])},
    {result['row_count']},
    PARSE_JSON('{payload_escaped}'),
    CURRENT_TIMESTAMP
);
"""
        execute_ddl_dml(conn, mode, insert_step)


def _queue_scheduled_commands(
    *,
    conn: Any,
    mode: str,
    steps: List[ValidationQueryStep],
    query_context: Dict[str, Any],
    params: Dict[str, Any],
) -> None:
    if mode not in {"snowflake_local", "snowflake_deployed"}:
        st.warning("Scheduling commands can only be published when connected to Snowflake.")
        return

    table_name = _command_table_name(params["table_name"], params["execution_environment"])
    ddl = f"""
CREATE OR REPLACE TABLE {table_name} (
    COMMAND_ID NUMBER(38, 0) AUTOINCREMENT START 1 INCREMENT 1 NOORDER,
    SQL_COMMAND VARCHAR(16777216),
    PRIMARY KEY (COMMAND_ID)
);
"""
    ok, msg = execute_ddl_dml(conn, mode, ddl)
    if not ok:
        st.error(f"Unable to ensure command table exists: {msg}")
        return

    ok, msg = execute_ddl_dml(conn, mode, f"TRUNCATE TABLE {table_name};")
    if not ok:
        st.error(f"Unable to reset command table: {msg}")
        return

    commands: List[str] = [
        f"SELECT 'Validation run for {params['table_name']} ({params['execution_environment']})' AS CONTEXT;"
    ]
    for step in steps:
        sql_text = step.sql_template.format(**query_context).strip()
        if not sql_text.endswith(";"):
            sql_text += ";"
        commands.append(sql_text)

    for command in commands:
        escaped = command.replace("'", "''")
        insert_sql = f"INSERT INTO {table_name} (SQL_COMMAND) VALUES ('{escaped}');"
        ok, msg = execute_ddl_dml(conn, mode, insert_sql)
        if not ok:
            st.error(f"Unable to insert scheduled command: {msg}")
            return

    st.success(
        "Scheduled task commands refreshed in DATA_VAULT_TEMP.MIGRATION_COPILOT for future warehouse execution."
    )


def _render_step(
    step_container: st.delta_generator.DeltaGenerator,
    *,
    conn: Any,
    mode: str,
    step: ValidationQueryStep,
    sql: str,
) -> Tuple[str, pd.DataFrame | None, str | None]:
    status_placeholder = step_container.empty()
    status_placeholder.info("Running...")
    if mode == "duckdb":
        status_placeholder.warning("Skipping in DuckDB mode (Snowflake-only SQL).")
        msg_df = pd.DataFrame({"info": ["Snowflake-only step; run against Snowflake."]})
        step_container.dataframe(msg_df, width="stretch", hide_index=True)
        return "skipped", msg_df, None
    try:
        df = run_query(conn, sql, mode)
        status_placeholder.success("Completed")
        if df.empty:
            step_container.warning("No rows returned for this step.")
        else:
            step_container.dataframe(df, width="stretch", hide_index=True)
        return "success", df, None
    except Exception as exc:  # pragma: no cover - surfaced via UI
        status_placeholder.error("Failed")
        step_container.exception(exc)
        return "error", None, str(exc)


def _fetch_run_history(conn: Any, mode: str, limit: int = 20) -> pd.DataFrame:
    run_table = _get_table_name("VALIDATION_MODEL_RUNS", mode)
    sql = f"""
SELECT RUN_ID, RUN_AT, RUN_TYPE, SCHEDULE_FLAG, RUN_ENVIRONMENT, RUN_STATUS, TABLE_NAME,
       SOURCE_DB, TARGET_DB, SAMPLE_SIZE, GENERATED_JSON
FROM {run_table}
ORDER BY RUN_AT DESC
LIMIT {limit};
"""
    try:
        return run_query(conn, sql, mode)
    except Exception:
        return pd.DataFrame()


def _fetch_run_results(conn: Any, mode: str, run_id: str) -> pd.DataFrame:
    result_table = _get_table_name("VALIDATION_MODEL_RESULTS", mode)
    sql = f"""
SELECT STEP_NAME, STATUS, ROW_COUNT, RESULT_DATA
FROM {result_table}
WHERE RUN_ID = {_escape_literal(run_id)}
ORDER BY STEP_NAME;
"""
    try:
        return run_query(conn, sql, mode)
    except Exception:
        return pd.DataFrame()


def main() -> None:
    conn = init_page()
    mode = get_runtime_mode()

    st.title("✅ Validation — Modelling")
    st.markdown(
        "Run the canonical Info Mart comparison script step-by-step, observe each SQL result, "
        "and persist an auditable record for day-over-day modelling validation."
    )

    if conn is None:
        st.error("No active database connection.")
        st.stop()

    _ensure_audit_tables(conn, mode)

    with st.form("modelling_validation_form"):
        col1, col2 = st.columns(2)
        table_name = _select_with_custom(
            col1,
            label="Table / View",
            options=TABLE_OPTIONS,
            default="DIM_SITE_ATTRIBUTES",
            key="table_name",
        )
        source_database = _select_with_custom(
            col1,
            label="Source Database",
            options=SOURCE_DB_OPTIONS,
            default="DATA_VAULT_TEMP",
            key="source_db",
        )
        source_schema = _select_with_custom(
            col1,
            label="Source Schema",
            options=SCHEMA_OPTIONS,
            default="INFO_MART",
            key="source_schema",
        )
        target_database = _select_with_custom(
            col2,
            label="Target Database",
            options=TARGET_DB_OPTIONS,
            default="_DATA_VAULT_DEV_CHRIS",
            key="target_db",
        )
        target_schema = _select_with_custom(
            col2,
            label="Target Schema",
            options=SCHEMA_OPTIONS,
            default="INFO_MART",
            key="target_schema",
        )
        sample_size = _select_int_with_custom(
            col1,
            label="Sample Size",
            options=SAMPLE_SIZE_OPTIONS,
            default=1000,
            key="sample_size",
        )
        join_key = _select_with_custom(
            col2,
            label="Join Key",
            options=JOIN_KEY_OPTIONS,
            default="SITE_ATTR_ID",
            key="join_key",
        )
        source_hw = _select_with_custom(
            col1,
            label="Source High-Watermark",
            options=HW_OPTIONS,
            default="DW_LAST_UPDATED_DATE",
            key="source_hw",
        )
        target_hw = _select_with_custom(
            col2,
            label="Target High-Watermark",
            options=HW_OPTIONS,
            default="SYSTEM_CREATE_DATE",
            key="target_hw",
        )
        exclude_cols = _select_exclude_columns(col1, "Default system columns", "exclude_cols")

        execution_environment = st.selectbox("Execution Environment", ENVIRONMENT_OPTIONS, index=0)

        schedule_flag = st.toggle("Schedule daily run?", value=False)
        run_type = st.selectbox("Run Type", ["manual", "scheduled"], index=0)
        if run_type == "scheduled":
            schedule_flag = True

        schedule_start = schedule_end = None
        if schedule_flag:
            schedule_start = st.date_input(
                "Schedule start date",
                value=date.today(),
                key="schedule_start",
            )
            schedule_end = st.date_input(
                "Schedule end date",
                value=date.today(),
                key="schedule_end",
            )

        generate_json = st.checkbox("Generate JSON summary (Step 8)", value=False)
        notes = st.text_area("Notes (optional)")
        submitted = st.form_submit_button("Run Validation")

    if submitted:
        params = {
            "table_name": table_name,
            "source_database": source_database,
            "source_schema": source_schema,
            "target_database": target_database,
            "target_schema": target_schema,
            "sample_size": sample_size,
            "join_key": join_key,
            "source_high_watermark": source_hw,
            "target_high_watermark": target_hw,
            "exclude_cols": exclude_cols,
            "schedule_flag": schedule_flag,
            "schedule_start_date": schedule_start.isoformat() if schedule_start else None,
            "schedule_end_date": schedule_end.isoformat() if schedule_end else None,
            "run_type": run_type,
            "generate_json": generate_json,
            "run_owner": st.session_state.get("user_email", "streamlit_user"),
            "execution_environment": execution_environment,
            "notes": notes or "",
        }

        query_context = _build_query_context(params)
        steps = get_modelling_validation_steps(include_json=generate_json)
        run_id = str(uuid.uuid4())
        results: List[Dict[str, Any]] = []
        run_status = "success"

        # Show complete script before execution
        with st.expander("📜 Complete Validation Script", expanded=False):
            complete_script = "\n\n-- Step separator\n\n".join(
                [f"-- Step {i}: {step.title}\n{step.sql_template.format(**query_context)}"
                 for i, step in enumerate(steps, start=1)]
            )
            st.code(complete_script, language="sql")

        st.markdown("### Run Status")
        for index, step in enumerate(steps, start=1):
            with st.expander(f"Step {index}: {step.title}", expanded=True):
                st.caption(step.description)
                step_sql = step.sql_template.format(**query_context)
                status, df, error = _render_step(
                    st.container(),
                    conn=conn,
                    mode=mode,
                    step=step,
                    sql=step_sql,
                )
                result_payload = df.to_dict(orient="records") if df is not None else None
                results.append(
                    {
                        "step": step.key,
                        "status": status,
                        "row_count": len(df) if df is not None else 0,
                        "data": result_payload,
                        "error": error,
                    }
                )
                if status == "error":
                    run_status = "error"

        _persist_run(conn, mode, run_id, run_status, params, results)
        st.success(f"Run {run_id} persisted with status '{run_status}'.")

        if params["schedule_flag"]:
            _queue_scheduled_commands(
                conn=conn,
                mode=mode,
                steps=steps,
                query_context=query_context,
                params=params,
            )

    st.markdown("### Recent Validation Runs")
    history_df = _fetch_run_history(conn, mode)
    if history_df.empty:
        st.info("No modelling validation runs recorded yet.")
    else:
        st.dataframe(history_df, width="stretch", hide_index=True)
        selected_run = st.selectbox("Inspect run results", history_df["RUN_ID"].tolist())
        if selected_run:
            run_results = _fetch_run_results(conn, mode, selected_run)
            if run_results.empty:
                st.warning("No step results found for the selected run.")
            else:
                def _format_result(value: Any) -> str:
                    if isinstance(value, (dict, list)):
                        return json.dumps(value, indent=2)
                    try:
                        return json.dumps(json.loads(value), indent=2)
                    except Exception:
                        return str(value)

                formatted = run_results.copy()
                if "RESULT_DATA" in formatted.columns:
                    formatted["RESULT_DATA"] = formatted["RESULT_DATA"].apply(_format_result)
                st.dataframe(formatted, width="stretch", hide_index=True)


main()
