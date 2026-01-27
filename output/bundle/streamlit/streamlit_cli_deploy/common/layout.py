"""
Shared layout and frame components for the Migration Copilot app.

This module contains common UI elements that appear on all pages:
- Runtime status display
- Database connection management
- Sidebar common elements
- Session state initialization

Usage in page files:
    from common.layout import init_page, get_db_connection, show_runtime_status
    
    # Call at the start of each page
    init_page()
    conn = get_db_connection()
"""

import os
import copy
from pathlib import Path
from typing import Any, Optional
import streamlit as st

from common.db import (
    detect_runtime,
    get_connection,
    RuntimeMode,
    DEFAULT_SNOWFLAKE_DATABASE,
    DEFAULT_SNOWFLAKE_SCHEMA,
    DEFAULT_INGESTION_METADATA_CONFIG,
)


def load_env_file(env_path: Path) -> None:
    """Load environment variables from a file (does not override existing)."""
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def init_session_state() -> None:
    """
    Initialize all session state variables used across the app.
    Call this once at app startup (in the entrypoint file).
    """
    # Runtime mode
    if "runtime_mode" not in st.session_state:
        st.session_state["runtime_mode"] = detect_runtime()
    
    # Database connection (will be set after successful connection)
    if "db_conn" not in st.session_state:
        st.session_state["db_conn"] = None
    
    # Test data configuration
    if "test_data_database" not in st.session_state:
        st.session_state["test_data_database"] = DEFAULT_SNOWFLAKE_DATABASE
    if "test_data_schema" not in st.session_state:
        st.session_state["test_data_schema"] = DEFAULT_SNOWFLAKE_SCHEMA
    if "ingestion_metadata_env" not in st.session_state:
        st.session_state["ingestion_metadata_env"] = "DEV"
    if "ingestion_metadata_config" not in st.session_state:
        st.session_state["ingestion_metadata_config"] = copy.deepcopy(
            DEFAULT_INGESTION_METADATA_CONFIG
        )


def init_environment() -> None:
    """
    Load environment files for local development.
    Call this once at app startup (in the entrypoint file).
    """
    load_env_file(Path(".env.local"))
    load_env_file(Path(".env.snowflake-dev"))
    load_env_file(Path(".env.duckdb"))


def get_runtime_mode() -> RuntimeMode:
    """Get the current runtime mode from session state."""
    if "runtime_mode" not in st.session_state:
        st.session_state["runtime_mode"] = detect_runtime()
    return st.session_state["runtime_mode"]


def get_db_connection() -> Any:
    """
    Get the database connection from session state.
    Creates connection if not already established.
    
    Returns:
        Database connection object (DuckDB, Snowflake connector, or Snowpark session)
    
    Raises:
        RuntimeError: If connection fails
    """
    if st.session_state.get("db_conn") is None:
        runtime_mode = get_runtime_mode()
        try:
            conn = get_connection(runtime_mode)
            st.session_state["db_conn"] = conn
        except Exception as e:
            raise RuntimeError(f"Failed to connect to database: {e}")
    
    return st.session_state["db_conn"]


def show_runtime_status() -> None:
    """Display runtime status in the sidebar."""
    runtime_mode = get_runtime_mode()
    
    st.sidebar.markdown("---")
    st.sidebar.caption("**Runtime**")
    
    if runtime_mode == "duckdb":
        st.sidebar.success("🦆 DuckDB (local)")
    elif runtime_mode == "snowflake_local":
        st.sidebar.success("❄️ Snowflake (local)")
    elif runtime_mode == "snowflake_deployed":
        st.sidebar.success("☁️ Snowflake (deployed)")


def show_connection_error(error: Exception) -> None:
    """Display a connection error and stop the app."""
    st.error(f"❌ Database connection failed: {error}")
    st.info("Check your credentials and try again.")
    st.stop()


def init_page(
    require_connection: bool = True,
    show_runtime: bool = True,
) -> Optional[Any]:
    """
    Initialize a page with common setup.
    
    Call this at the start of each page file to ensure consistent behavior.
    
    Args:
        require_connection: If True, establish DB connection and stop on failure
        show_runtime: If True, show runtime status in sidebar
    
    Returns:
        Database connection if require_connection is True, else None
    """
    conn = None
    
    if require_connection:
        try:
            conn = get_db_connection()
        except RuntimeError as e:
            show_connection_error(e)
    
    if show_runtime:
        show_runtime_status()
    
    return conn


# =============================================================================
# App-wide configuration
# =============================================================================

APP_TITLE = "Migration Copilot"
APP_ICON = "🚀"

def set_page_config() -> None:
    """
    Set the Streamlit page configuration.
    Call this once at the very start of the entrypoint file.
    
    Note: st.set_page_config must be the first Streamlit command.
    """
    st.set_page_config(
        page_title=APP_TITLE,
        page_icon=APP_ICON,
        layout="wide",
        initial_sidebar_state="expanded",
    )
