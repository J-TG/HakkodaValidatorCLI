"""
Database connection helpers with auto-detection of runtime mode.

⚠️  WAREHOUSE RUNTIME CONSTRAINTS (CRITICAL)
============================================
Snowflake Streamlit runs inside warehouse infrastructure with strict rules:
- Only Anaconda-managed packages are available
- No pip install at runtime
- No dynamic dependency resolution
- DuckDB is NOT available in Snowflake warehouse runtime

This module enforces a hard boundary:
- DuckDB imports are LAZY and GUARDED - only imported when runtime_mode == "duckdb"
- Snowflake runtime code NEVER imports duckdb, even conditionally
- Environment detection happens BEFORE any local-only imports

Runtime modes:
- "snowflake_deployed": Running inside Snowflake Streamlit (uses get_active_session)
- "snowflake_local": Running locally with Snowflake credentials (uses connector)
- "duckdb": Running locally with DuckDB (in-memory or file) - LOCAL ONLY

Principle: If it cannot run inside a Snowflake warehouse with Anaconda-managed
dependencies, it is not part of the real application — only a local convenience.
"""

import os
import re
from copy import deepcopy
from typing import Literal, Optional, Any, List, Tuple, Dict
import streamlit as st
import pandas as pd

# =============================================================================
# CRITICAL: Runtime mode detection must happen BEFORE any local-only imports
# =============================================================================
# DuckDB is LOCAL-ONLY. Never import it at module level.
# The get_duckdb_connection() function contains a guarded import.

# Default table prefix for Snowflake queries (configurable via UI)
DEFAULT_SNOWFLAKE_DATABASE = "DATA_VAULT_DEV"
DEFAULT_SNOWFLAKE_SCHEMA = "INFO_MART"

METADATA_CONFIG_TABLE_NAME = "METADATA_CONFIG_TABLE_ELT"
DEFAULT_INGESTION_METADATA_CONFIG: Dict[str, Dict[str, str]] = {
    "DEV": {"database": "STAGE_DEV", "schema": "ELT"},
    "TEST": {"database": "STAGE_TEST", "schema": "ELT"},
    "UAT": {"database": "STAGE_UAT", "schema": "ELT"},
    "PROD": {"database": "STAGE_PROD", "schema": "ELT"},
}
METADATA_ENVIRONMENTS = tuple(DEFAULT_INGESTION_METADATA_CONFIG.keys())

RuntimeMode = Literal["snowflake_deployed", "snowflake_local", "duckdb"]


def get_snowflake_table_prefix() -> str:
    """Get the current Snowflake table prefix from session state or defaults."""
    db = st.session_state.get("test_data_database", DEFAULT_SNOWFLAKE_DATABASE)
    schema = st.session_state.get("test_data_schema", DEFAULT_SNOWFLAKE_SCHEMA)
    # Strip whitespace and ensure non-empty
    db = (db or DEFAULT_SNOWFLAKE_DATABASE).strip()
    schema = (schema or DEFAULT_SNOWFLAKE_SCHEMA).strip()
    return f"{db}.{schema}"


def detect_runtime() -> RuntimeMode:
    """
    Detect the runtime mode based on environment and available sessions.
    
    Priority:
    1. RUNTIME_MODE env var (set by Makefile targets)
    2. Try Snowflake get_active_session() (deployed in Snowflake)
    3. Check st.secrets for Snowflake creds (local Snowflake)
    4. Fallback to DuckDB
    """
    # Check explicit RUNTIME_MODE from Makefile
    explicit_mode = os.getenv("RUNTIME_MODE", "").lower()
    if explicit_mode == "duckdb":
        return "duckdb"
    if explicit_mode == "snowflake_local":
        return "snowflake_local"
    if explicit_mode == "snowflake_deployed":
        return "snowflake_deployed"
    
    # Try Snowflake deployed session
    try:
        from snowflake.snowpark.context import get_active_session
        session = get_active_session()
        if session is not None:
            return "snowflake_deployed"
    except Exception:
        pass
    
    # Check for Snowflake secrets (local Snowflake mode)
    try:
        snow_secrets = st.secrets.get("snowflake", {})
        if snow_secrets.get("account") and snow_secrets.get("user"):
            return "snowflake_local"
    except Exception:
        pass
    
    # Fallback to DuckDB
    return "duckdb"


@st.cache_resource
def get_duckdb_connection():
    """
    Create and cache a DuckDB connection.
    
    ⚠️  LOCAL-ONLY: This function must NEVER be called in Snowflake warehouse runtime.
    The import is guarded here to prevent warehouse runtime failures.
    
    Raises:
        RuntimeError: If called in Snowflake deployed mode (warehouse runtime)
    """
    # GUARD: Prevent accidental calls in warehouse runtime
    current_mode = os.getenv("RUNTIME_MODE", "").lower()
    if current_mode == "snowflake_deployed":
        raise RuntimeError(
            "DuckDB is not available in Snowflake warehouse runtime. "
            "This is a local-only feature. Check runtime mode before calling."
        )
    
    # LAZY IMPORT: DuckDB is only imported when this function is actually called
    # This prevents import failures in Snowflake warehouse where duckdb doesn't exist
    import duckdb
    
    duckdb_database = os.getenv("DUCKDB_DATABASE", ":memory:")
    duckdb_read_only = os.getenv("DUCKDB_READ_ONLY", "false").lower() in {"1", "true", "yes"}
    conn = duckdb.connect(database=duckdb_database, read_only=duckdb_read_only)
    return conn


@st.cache_resource
def get_snowflake_connector():
    """
    Create and cache a Snowflake connector connection.
    
    ℹ️  LOCAL DEV ONLY: This uses snowflake-connector-python for local development.
    In Snowflake warehouse runtime, use get_snowpark_session() instead.
    The snowflake.connector package is available in Anaconda but Snowpark is preferred.
    """
    # LAZY IMPORT: Keep imports inside function for cleaner dependency management
    import snowflake.connector
    
    try:
        snow_secrets = st.secrets.get("snowflake", {})
    except Exception:
        snow_secrets = {}
    
    return snowflake.connector.connect(
        account=snow_secrets.get("account", os.getenv("SF_ACCOUNT", "")),
        user=snow_secrets.get("user", os.getenv("SF_USER", "")),
        password=snow_secrets.get("password", os.getenv("SF_PASSWORD", "")),
        warehouse=snow_secrets.get("warehouse", os.getenv("SF_WAREHOUSE")) or None,
        database=snow_secrets.get("database", os.getenv("SF_DATABASE")) or None,
        schema=snow_secrets.get("schema", os.getenv("SF_SCHEMA")) or None,
        role=snow_secrets.get("role", os.getenv("SF_ROLE")) or None,
    )


def get_snowpark_session():
    """Get the active Snowpark session (for deployed Snowflake Streamlit)."""
    from snowflake.snowpark.context import get_active_session
    return get_active_session()


@st.cache_data(ttl=300)
def run_query(_conn: Any, query: str, mode: RuntimeMode) -> pd.DataFrame:
    """
    Execute a query and return results as a DataFrame.
    
    Args:
        _conn: Database connection (DuckDB, Snowflake connector, or Snowpark session)
        query: SQL query string
        mode: Runtime mode for proper query execution
    
    Returns:
        pandas DataFrame with results
    """
    if mode == "duckdb":
        return _conn.execute(query).fetchdf()
    
    elif mode == "snowflake_deployed":
        # Snowpark session
        return _conn.sql(query).to_pandas()
    
    elif mode == "snowflake_local":
        # Snowflake connector
        cur = _conn.cursor()
        try:
            cur.execute(query)
            try:
                return cur.fetch_pandas_all()
            except Exception:
                rows = cur.fetchall()
                cols = [c[0] for c in cur.description]
                return pd.DataFrame(rows, columns=cols)
        finally:
            cur.close()
    
    raise ValueError(f"Unknown mode: {mode}")


def get_testtable_query(mode: RuntimeMode) -> str:
    """Get the appropriate TESTTABLE query based on runtime mode."""
    if mode == "duckdb":
        return "SELECT * FROM TESTTABLE LIMIT 100"
    else:
        prefix = get_snowflake_table_prefix()
        return f"SELECT * FROM {prefix}.TESTTABLE LIMIT 100"


def get_connection(mode: RuntimeMode) -> Any:
    """Get the appropriate database connection based on runtime mode."""
    if mode == "duckdb":
        return get_duckdb_connection()
    elif mode == "snowflake_deployed":
        return get_snowpark_session()
    elif mode == "snowflake_local":
        return get_snowflake_connector()
    raise ValueError(f"Unknown mode: {mode}")


def _ensure_ingestion_configs() -> Dict[str, Dict[str, str]]:
    configs = st.session_state.get("ingestion_metadata_config")
    if not configs:
        configs = {env: cfg.copy() for env, cfg in DEFAULT_INGESTION_METADATA_CONFIG.items()}
        st.session_state["ingestion_metadata_config"] = configs
    return configs


def get_ingestion_metadata_env() -> str:
    env = st.session_state.get("ingestion_metadata_env", METADATA_ENVIRONMENTS[0])
    env = (env or METADATA_ENVIRONMENTS[0]).upper()
    if env not in METADATA_ENVIRONMENTS:
        env = METADATA_ENVIRONMENTS[0]
    st.session_state["ingestion_metadata_env"] = env
    return env


def get_ingestion_metadata_env_config(env: Optional[str] = None) -> Dict[str, str]:
    configs = _ensure_ingestion_configs()
    target_env = env.upper() if env else get_ingestion_metadata_env()
    if target_env not in configs:
        default_cfg = DEFAULT_INGESTION_METADATA_CONFIG.get(
            target_env,
            {"database": f"STAGE_{target_env}", "schema": "ELT"},
        )
        configs[target_env] = default_cfg.copy()
        st.session_state["ingestion_metadata_config"] = configs
    return configs[target_env]


def _normalize_metadata_location(env: str, config: Dict[str, str]) -> Dict[str, str]:
    database = (config.get("database") or f"STAGE_{env}").strip()
    schema = (config.get("schema") or "ELT").strip()
    return {"database": database, "schema": schema}


def get_ingestion_metadata_table(mode: RuntimeMode) -> str:
    if mode == "duckdb":
        return METADATA_CONFIG_TABLE_NAME
    env = get_ingestion_metadata_env()
    config = _normalize_metadata_location(env, get_ingestion_metadata_env_config(env))
    return f"{config['database']}.{config['schema']}.{METADATA_CONFIG_TABLE_NAME}"


def get_ingestion_metadata_table_path(env: Optional[str] = None) -> str:
    env_name = env.upper() if env else get_ingestion_metadata_env()
    config = _normalize_metadata_location(env_name, get_ingestion_metadata_env_config(env_name))
    return f"{config['database']}.{config['schema']}.{METADATA_CONFIG_TABLE_NAME}"


_SQL_STRING_LITERAL_SPLIT = re.compile(r"('(?:''|[^'])*')")
_DUCKDB_TYPE_REPLACEMENTS = (
    (re.compile(r"\bVARIANT\b", re.IGNORECASE), "JSON"),
    (re.compile(r"\bTIMESTAMP_NTZ\s*\(\s*\d+\s*\)", re.IGNORECASE), "TIMESTAMP"),
    (re.compile(r"\bTIMESTAMP_NTZ\b", re.IGNORECASE), "TIMESTAMP"),
    (re.compile(r"\bNUMBER\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)", re.IGNORECASE), r"DECIMAL(\1,\2)"),
    (re.compile(r"\bNUMBER\b", re.IGNORECASE), "DOUBLE"),
)
_DUCKDB_FUNC_REPLACEMENTS = (
    # PARSE_JSON(x) → CAST(x AS JSON) so DuckDB can ingest the payload literal
    (re.compile(r"\bPARSE_JSON\s*\(([^)]+)\)", re.IGNORECASE), r"CAST(\1 AS JSON)"),
)


def _substitute_outside_literals(sql: str, pattern: "re.Pattern[str]", replacement: str) -> str:
    parts = _SQL_STRING_LITERAL_SPLIT.split(sql)
    for idx in range(0, len(parts), 2):
        parts[idx] = pattern.sub(replacement, parts[idx])
    return "".join(parts)


def _normalize_create_for_duckdb(sql: str) -> str:
    stripped = sql.lstrip()
    if not stripped.upper().startswith("CREATE"):
        return sql
    normalized = sql
    for pattern, repl in _DUCKDB_TYPE_REPLACEMENTS:
        normalized = _substitute_outside_literals(normalized, pattern, repl)
    return normalized


def _normalize_for_duckdb(sql: str) -> str:
    """Apply DuckDB-specific normalizations for both DDL and DML."""

    normalized = _normalize_create_for_duckdb(sql)
    for pattern, repl in _DUCKDB_FUNC_REPLACEMENTS:
        normalized = _substitute_outside_literals(normalized, pattern, repl)
    return normalized


# =============================================================================
# Test Data Setup Functions
# =============================================================================

def execute_ddl_dml(conn: Any, mode: RuntimeMode, sql: str) -> Tuple[bool, str]:
    """
    Execute a DDL or DML statement.
    
    Returns:
        Tuple of (success: bool, message: str)
    """
    try:
        if mode == "duckdb":
            normalized_sql = _normalize_for_duckdb(sql)
            conn.execute(normalized_sql)
            return True, "OK"
        
        elif mode == "snowflake_deployed":
            # Snowpark session
            conn.sql(sql).collect()
            return True, "OK"
        
        elif mode == "snowflake_local":
            # Snowflake connector
            cur = conn.cursor()
            try:
                cur.execute(sql)
                return True, "OK"
            finally:
                cur.close()
        
        return False, f"Unknown mode: {mode}"
    except Exception as e:
        return False, str(e)


def setup_test_tables(
    conn: Any,
    mode: RuntimeMode,
    schema_prefix: str = "",
) -> List[Tuple[str, bool, str]]:
    """
    Create and populate all test tables.
    
    Args:
        conn: Database connection
        mode: Runtime mode
        schema_prefix: Schema prefix for Snowflake (e.g., "DATA_VAULT_DEV.INFO_MART")
    
    Returns:
        List of (table_name, success, message) tuples
    """
    from common.test_data import TEST_TABLES, format_ddl, format_dml
    
    results = []
    
    # For Snowflake modes, set database/schema context first
    if mode in ("snowflake_local", "snowflake_deployed") and schema_prefix:
        parts = schema_prefix.split(".")
        if len(parts) >= 2:
            db_name, schema_name = parts[0], parts[1]
            # Set context to avoid fully-qualified name issues
            execute_ddl_dml(conn, mode, f'USE DATABASE "{db_name}"')
            execute_ddl_dml(conn, mode, f'USE SCHEMA "{schema_name}"')
    
    for table_def in TEST_TABLES:
        table_name = table_def["name"]
        
        # For Snowflake with context set, use just the table name
        if mode in ("snowflake_local", "snowflake_deployed") and schema_prefix:
            effective_prefix = ""  # Context already set via USE statements
        else:
            effective_prefix = schema_prefix
        
        # Execute DDL
        ddl = format_ddl(table_name, effective_prefix)
        ddl_ok, ddl_msg = execute_ddl_dml(conn, mode, ddl)
        
        if not ddl_ok:
            results.append((f"{table_name} (DDL)", False, ddl_msg))
            continue
        
        # Execute DML
        dml = format_dml(table_name, effective_prefix)
        dml_ok, dml_msg = execute_ddl_dml(conn, mode, dml)
        
        if dml_ok:
            results.append((table_name, True, "Created and populated"))
        else:
            results.append((f"{table_name} (DML)", False, dml_msg))
    
    return results


def check_test_tables_exist(
    conn: Any,
    mode: RuntimeMode,
    schema_prefix: str = "",
    query_log: Optional[List[str]] = None,
) -> List[Tuple[str, bool, int, Optional[str]]]:
    """
    Check which test tables exist, their row counts, and max date.
    
    Returns:
        List of (table_name, exists, row_count, max_date) tuples
        max_date is a string representation or None if not available
    """
    from common.test_data import get_table_names, get_table_date_column
    
    effective_prefix = schema_prefix or ""
    if not effective_prefix and mode in ("snowflake_local", "snowflake_deployed"):
        effective_prefix = get_snowflake_table_prefix()

    results = []
    
    for table_name in get_table_names():
        if effective_prefix:
            full_name = f"{effective_prefix}.{table_name}"
        else:
            full_name = table_name
        
        try:
            # Get row count
            query = f"SELECT COUNT(*) as cnt FROM {full_name};"
            if query_log is not None:
                query_log.append(query)
            df = run_query(conn, query, mode)
            count = int(df.iloc[0, 0])
            
            # Get max date if date column is defined
            max_date = None
            date_col = get_table_date_column(table_name)
            if date_col and count > 0:
                try:
                    date_query = f"SELECT MAX({date_col}) as max_dt FROM {full_name};"
                    if query_log is not None:
                        query_log.append(date_query)
                    date_df = run_query(conn, date_query, mode)
                    max_val = date_df.iloc[0, 0]
                    if max_val is not None:
                        max_date = str(max_val)
                except Exception:
                    pass  # If date query fails, just skip it
            
            results.append((table_name, True, count, max_date))
        except Exception:
            results.append((table_name, False, 0, None))
    
    return results
