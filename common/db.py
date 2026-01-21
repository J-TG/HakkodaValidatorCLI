"""
Database connection helpers with auto-detection of runtime mode.

Runtime modes:
- "snowflake_deployed": Running inside Snowflake Streamlit (uses get_active_session)
- "snowflake_local": Running locally with Snowflake credentials (uses connector)
- "duckdb": Running locally with DuckDB (in-memory or file)
"""

import os
from typing import Literal, Optional, Any, List, Tuple
import streamlit as st
import pandas as pd

# Default table prefix for Snowflake queries (configurable via UI)
DEFAULT_SNOWFLAKE_DATABASE = "DATA_VAULT_DEV"
DEFAULT_SNOWFLAKE_SCHEMA = "INFO_MART"

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
    """Create and cache a DuckDB connection (no auto-created tables)."""
    import duckdb
    
    duckdb_database = os.getenv("DUCKDB_DATABASE", ":memory:")
    duckdb_read_only = os.getenv("DUCKDB_READ_ONLY", "false").lower() in {"1", "true", "yes"}
    conn = duckdb.connect(database=duckdb_database, read_only=duckdb_read_only)
    return conn


@st.cache_resource
def get_snowflake_connector():
    """Create and cache a Snowflake connector connection (for local dev)."""
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
            conn.execute(sql)
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
) -> List[Tuple[str, bool, int]]:
    """
    Check which test tables exist and their row counts.
    
    Returns:
        List of (table_name, exists, row_count) tuples
    """
    from common.test_data import get_table_names
    
    results = []
    
    for table_name in get_table_names():
        if schema_prefix:
            full_name = f"{schema_prefix}.{table_name}"
        else:
            full_name = table_name
        
        try:
            query = f"SELECT COUNT(*) as cnt FROM {full_name}"
            df = run_query(conn, query, mode)
            count = int(df.iloc[0, 0])
            results.append((table_name, True, count))
        except Exception:
            results.append((table_name, False, 0))
    
    return results
