"""
Test data definitions - single source of truth for DDL and DML across all runtime modes.

This module defines all test tables and sample data used for development and testing.
The same definitions are used for:
- DuckDB (local in-memory)
- Snowflake (local connector)
- Snowflake (deployed via Snowpark)
"""

from typing import List, Dict, Any

# =============================================================================
# Table Definitions
# =============================================================================

# Each table definition contains:
# - name: Table name (without schema prefix)
# - ddl: CREATE TABLE statement (use {schema_prefix} placeholder for Snowflake)
# - dml: INSERT statements for sample data
# - description: Human-readable description

TEST_TABLES: List[Dict[str, Any]] = [
    {
        "name": "TESTTABLE",
        "description": "Call center reference data (TPC-DS style)",
        "ddl": """
CREATE OR REPLACE TABLE {table_ref} (
    CC_CALL_CENTER_SK BIGINT,
    CC_CALL_CENTER_ID VARCHAR(16),
    CC_REC_START_DATE DATE,
    CC_REC_END_DATE DATE,
    CC_CLOSED_DATE_SK BIGINT,
    CC_OPEN_DATE_SK BIGINT,
    CC_NAME VARCHAR(50),
    CC_CLASS VARCHAR(50),
    CC_EMPLOYEES BIGINT,
    CC_SQ_FT BIGINT,
    CC_HOURS VARCHAR(20),
    CC_MANAGER VARCHAR(40),
    CC_MKT_ID BIGINT,
    CC_MKT_CLASS VARCHAR(50),
    CC_MKT_DESC VARCHAR(100),
    CC_MARKET_MANAGER VARCHAR(40),
    CC_DIVISION BIGINT,
    CC_DIVISION_NAME VARCHAR(50),
    CC_COMPANY BIGINT,
    CC_COMPANY_NAME VARCHAR(50),
    CC_STREET_NUMBER VARCHAR(10),
    CC_STREET_NAME VARCHAR(60),
    CC_STREET_TYPE VARCHAR(15),
    CC_SUITE_NUMBER VARCHAR(10),
    CC_CITY VARCHAR(60),
    CC_COUNTY VARCHAR(30),
    CC_STATE VARCHAR(2),
    CC_ZIP VARCHAR(10),
    CC_COUNTRY VARCHAR(20),
    CC_GMT_OFFSET DOUBLE,
    CC_TAX_PERCENTAGE DOUBLE
);
""",
        "dml": """
INSERT INTO {table_ref} (
    CC_CALL_CENTER_SK, CC_CALL_CENTER_ID, CC_REC_START_DATE, CC_REC_END_DATE,
    CC_NAME, CC_CLASS, CC_EMPLOYEES, CC_SQ_FT, CC_HOURS, CC_MANAGER,
    CC_MKT_ID, CC_DIVISION, CC_DIVISION_NAME, CC_COMPANY, CC_COMPANY_NAME,
    CC_CITY, CC_COUNTY, CC_STATE, CC_ZIP, CC_COUNTRY, CC_GMT_OFFSET, CC_TAX_PERCENTAGE
) VALUES
    (1, 'AAAAAAAABAAAAAAA', '1998-01-01', NULL, 'NY Metro', 'large', 150, 25000, '8AM-8PM', 'John Smith', 1, 1, 'East', 1, 'Acme Corp', 'New York', 'New York', 'NY', '10001', 'United States', -5.00, 0.08),
    (2, 'AAAAAAAACAAAAAAA', '1998-01-01', NULL, 'West Coast Hub', 'medium', 75, 12000, '6AM-6PM', 'Jane Doe', 2, 2, 'West', 1, 'Acme Corp', 'Los Angeles', 'Los Angeles', 'CA', '90001', 'United States', -8.00, 0.09),
    (3, 'AAAAAAAADAAAAAAA', '1998-01-01', NULL, 'Midwest Support', 'small', 30, 5000, '9AM-5PM', 'Bob Wilson', 3, 3, 'Central', 1, 'Acme Corp', 'Chicago', 'Cook', 'IL', '60601', 'United States', -6.00, 0.07),
    (4, 'AAAAAAAAEAAAAAAA', '2000-01-01', '2005-12-31', 'Southeast Center', 'medium', 50, 8000, '7AM-7PM', 'Alice Brown', 4, 4, 'South', 1, 'Acme Corp', 'Atlanta', 'Fulton', 'GA', '30301', 'United States', -5.00, 0.06),
    (5, 'AAAAAAAAFAAAAAAA', '2005-01-01', NULL, 'Texas Regional', 'large', 120, 20000, '7AM-9PM', 'Carlos Garcia', 5, 4, 'South', 1, 'Acme Corp', 'Dallas', 'Dallas', 'TX', '75201', 'United States', -6.00, 0.08);
""",
    },
    {
        "name": "SAMPLE_DATA",
        "description": "Simple sample table for quick tests",
        "ddl": """
CREATE OR REPLACE TABLE {table_ref} (
    ID INTEGER,
    NAME VARCHAR(50),
    VALUE DOUBLE,
    CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
""",
        "dml": """
INSERT INTO {table_ref} (ID, NAME, VALUE) VALUES
    (1, 'alice', 1.23),
    (2, 'bob', 4.56),
    (3, 'carol', 7.89),
    (4, 'david', 10.11),
    (5, 'eve', 12.13);
""",
    },
    {
        "name": "DAILY_METRICS",
        "description": "Sample time-series metrics data",
        "ddl": """
CREATE OR REPLACE TABLE {table_ref} (
    METRIC_DATE DATE,
    METRIC_NAME VARCHAR(50),
    METRIC_VALUE DOUBLE,
    REGION VARCHAR(20)
);
""",
        "dml": """
INSERT INTO {table_ref} (METRIC_DATE, METRIC_NAME, METRIC_VALUE, REGION) VALUES
    ('2025-01-01', 'revenue', 10500.00, 'East'),
    ('2025-01-01', 'revenue', 8200.00, 'West'),
    ('2025-01-01', 'revenue', 6100.00, 'Central'),
    ('2025-01-02', 'revenue', 11200.00, 'East'),
    ('2025-01-02', 'revenue', 8900.00, 'West'),
    ('2025-01-02', 'revenue', 6400.00, 'Central'),
    ('2025-01-03', 'revenue', 10800.00, 'East'),
    ('2025-01-03', 'revenue', 9100.00, 'West'),
    ('2025-01-03', 'revenue', 6700.00, 'Central'),
    ('2025-01-01', 'calls', 1200, 'East'),
    ('2025-01-01', 'calls', 950, 'West'),
    ('2025-01-01', 'calls', 620, 'Central'),
    ('2025-01-02', 'calls', 1350, 'East'),
    ('2025-01-02', 'calls', 1020, 'West'),
    ('2025-01-02', 'calls', 680, 'Central'),
    ('2025-01-03', 'calls', 1280, 'East'),
    ('2025-01-03', 'calls', 990, 'West'),
    ('2025-01-03', 'calls', 710, 'Central');
""",
    },
]


def get_table_names() -> List[str]:
    """Return list of all test table names."""
    return [t["name"] for t in TEST_TABLES]


def get_table_definition(table_name: str) -> Dict[str, Any]:
    """Get definition for a specific table."""
    for t in TEST_TABLES:
        if t["name"].upper() == table_name.upper():
            return t
    raise ValueError(f"Unknown table: {table_name}")


def format_ddl(table_name: str, schema_prefix: str = "") -> str:
    """
    Format DDL for a table with optional schema prefix.
    
    Args:
        table_name: Name of the table
        schema_prefix: Optional schema prefix (e.g., "DATA_VAULT_DEV.INFO_MART")
    
    Returns:
        Formatted DDL statement
    """
    defn = get_table_definition(table_name)
    if schema_prefix:
        table_ref = f"{schema_prefix}.{table_name}"
    else:
        table_ref = table_name
    return defn["ddl"].format(table_ref=table_ref).strip()


def format_dml(table_name: str, schema_prefix: str = "") -> str:
    """
    Format DML for a table with optional schema prefix.
    
    Args:
        table_name: Name of the table
        schema_prefix: Optional schema prefix (e.g., "DATA_VAULT_DEV.INFO_MART")
    
    Returns:
        Formatted DML statement
    """
    defn = get_table_definition(table_name)
    if schema_prefix:
        table_ref = f"{schema_prefix}.{table_name}"
    else:
        table_ref = table_name
    return defn["dml"].format(table_ref=table_ref).strip()


def get_all_ddl(schema_prefix: str = "") -> List[str]:
    """Get all DDL statements for all test tables."""
    return [format_ddl(t["name"], schema_prefix) for t in TEST_TABLES]


def get_all_dml(schema_prefix: str = "") -> List[str]:
    """Get all DML statements for all test tables."""
    return [format_dml(t["name"], schema_prefix) for t in TEST_TABLES]
