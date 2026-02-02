"""
Test data definitions - single source of truth for DDL and DML across all runtime modes.

This module defines all test tables and sample data used for development and testing.
The same definitions are used for:
- DuckDB (local in-memory)
- Snowflake (local connector)
- Snowflake (deployed via Snowpark)
"""

from typing import List, Dict, Any, Optional


def json_literal(payload: str) -> str:
    """Return a Snowflake-friendly JSON literal that also parses in DuckDB."""

    # Use dollar-quoting to avoid escaping and let DuckDB CAST handle it.
    return f"PARSE_JSON($${payload}$$)"

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
        "date_column": "CC_REC_START_DATE",  # Column to use for max date check
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
        "date_column": "CREATED_AT",  # Column to use for max date check
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
        "date_column": "METRIC_DATE",  # Column to use for max date check
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
    {
        "name": "METADATA_CONFIG_TABLE_ELT",
        "description": "Metadata configuration for ELT pipeline - stores parameters for Azure Data Factory and Snowflake data loading",
        "date_column": "STAGE_LAST_LOAD_DT",  # Column to use for max date check
        "ddl": """
CREATE OR REPLACE TABLE {table_ref} (
    METADATA_CONFIG_KEY VARCHAR(16777216),
    LOGICAL_NAME VARCHAR(16777216),
    FILE_LANDING_SCHEMA VARCHAR(16777216),
    FILE_WILDCARD_PATTERN VARCHAR(16777216),
    FILE_TABLE_NAME VARCHAR(16777216),
    FILE_EXTENSION VARCHAR(16777216),
    SHEET_NAME VARCHAR(16777216),
    SOURCE_TYPE VARCHAR(16777216),
    ENABLED VARCHAR(16777216),
    BLOB_LAST_LOAD_DT VARCHAR(16777216),
    STAGE_LAST_LOAD_DT VARCHAR(16777216),
    TARGET_LAST_LOAD_DT VARCHAR(16777216),
    ROW_COUNT VARCHAR(16777216),
    ERROR_STATUS VARCHAR(16777216),
    FILE_FORMAT VARCHAR(50),
    HAS_HEADER VARCHAR(16777216) DEFAULT 'Y',
    DATABASE_NAME VARCHAR(255),
    SCHEMA_NAME VARCHAR(255),
    SOURCE_TABLE_NAME VARCHAR(255),
    DELTA_COLUMN VARCHAR(255),
    DELTA_VALUE VARCHAR(255),
    CHANGE_TRACKING_TYPE VARCHAR(255),
    PRIORITY_FLAG BOOLEAN,
    PAYLOAD VARIANT,
    SERVER_NAME VARCHAR(100),
    FILE_SERVER_LOAD_TYPE VARCHAR(16777216),
    LAST_MODIFIED_DATE_LOADED VARCHAR(16777216),
    FILESTAMP_FORMAT VARCHAR(16777216),
    AUTO_INGEST VARCHAR(16777216),
    FILE_SERVER_FILEPATH VARCHAR(16777216),
    FIXED_WIDTH_FILETYPE VARCHAR(16777216),
    REGEX_PATTERN VARCHAR(16777216) DEFAULT ''
);
""",
        "dml": """
INSERT INTO {table_ref} (
    METADATA_CONFIG_KEY, LOGICAL_NAME, FILE_LANDING_SCHEMA, FILE_WILDCARD_PATTERN,
    FILE_TABLE_NAME, FILE_EXTENSION, SHEET_NAME, SOURCE_TYPE, ENABLED,
    BLOB_LAST_LOAD_DT, STAGE_LAST_LOAD_DT, TARGET_LAST_LOAD_DT, ROW_COUNT,
    ERROR_STATUS, FILE_FORMAT, HAS_HEADER, DATABASE_NAME, SCHEMA_NAME,
    SOURCE_TABLE_NAME, DELTA_COLUMN, DELTA_VALUE, CHANGE_TRACKING_TYPE,
    PRIORITY_FLAG, PAYLOAD, SERVER_NAME, FILE_SERVER_LOAD_TYPE,
    LAST_MODIFIED_DATE_LOADED, FILESTAMP_FORMAT, AUTO_INGEST,
    FILE_SERVER_FILEPATH, FIXED_WIDTH_FILETYPE, REGEX_PATTERN
) VALUES
    ('h8c1d2e3-8901-1j25-6g7h-23456789abcd', NULL, NULL, 'SCANDS_Staging', NULL, NULL, NULL, 'csv', '1', '2025-06-06T04:55:08.3666865', '2025-12-22 08:09:02.337', '2025-12-22 08:10:59.441 -0800', '730325', '0', 'HEART_PARQUET_FORMAT', 'Y', 'SCANDS_PROD', 'dbo', 'B_MemberApplicationHistory', 'LOAD_DATETIME', '2025-12-22 08:09:02.337', 'FULL', false, NULL, 'DS-STAGE-TEST', NULL, NULL, NULL, NULL, NULL, NULL, NULL),
    ('g7b0c1d2-7890-0i14-5f6g-123456789abc', 'LEGACY', NULL, NULL, NULL, NULL, NULL, 'SQL', '1', '2025-12-22T09:28:17.3462705', '2025-12-22 09:28:08.590', '2025-12-22 09:28:53.206 -0800', '3889', '0', 'HEART_PARQUET_FORMAT', NULL, 'SCANDS_PROD', 'dbo', 'D_Site_Attributes', 'LOAD_DATETIME', '2025-12-22 09:28:08.590', 'FULL', false, NULL, 'BI-RPT-TEST', NULL, NULL, NULL, NULL, NULL, NULL, NULL),
    ('fff79ec3-e01a-4673-b407-7ed32ef12ef3', 'LEGACY', NULL, NULL, NULL, NULL, NULL, 'SQL', '1', '2025-12-31T09:20:30.1731745', '2025-12-31 09:20:23.577', '2025-12-31 09:21:36.169 -0800', '3', '0', 'HEART_PARQUET_FORMAT', 'Y', 'SCANDS_STAGING', 'dbo', 'CMSLTIValue', 'LOAD_DATETIME', '2025-12-31 09:20:23.577', 'FULL', false, NULL, 'DS-STAGE-TEST', NULL, NULL, NULL, NULL, NULL, NULL, NULL),
    ('ffe1fe02-3db9-4536-81bb-ea167e78de33', 'Excel_Test_CSV', 'SANDBOX', 'SCAN_CareLinx_Member_Utilization_', 'CareLinx_Member_Utilization_CSV', 'csv', 'SCAN_CareLinx_Member_Utilizatio', 'csv', '1', '1900-01-01', '1900-01-01', '1900-01-01', 'N/A', '0', 'HEART_MOCK_DDL_PIPE_COMMA', 'Y', NULL, NULL, NULL, NULL, NULL, NULL, false, NULL, NULL, NULL, '2025-11-06T19:43:55.5274353Z', NULL, NULL, '\\san02\\EDW_SHARE\\FileSharing\\Hakkoda\\Product\\CSV', NULL, NULL),
    ('ffd63193-c072-40bf-89ae-d46358e84da9', 'Excel_Test_CSV', 'SANDBOX', 'HV5_PV5 VH', 'HealthyFoodCard_VH_csv', 'csv', 'Purse Activity Report', 'csv', '1', '1900-01-01', '1900-01-01', '1900-01-01', 'N/A', '0', 'HEART_MOCK_DDL_PIPE_COMMA', 'Y', NULL, NULL, NULL, NULL, NULL, NULL, false, NULL, NULL, NULL, '1900-01-01', NULL, NULL, '\\san02\\EDW_SHARE\\FileSharing\\Hakkoda\\Product\\CSV', NULL, NULL),
    ('ff645be1-3e3d-42cc-8dbc-fe8b76fbb920', NULL, 'MMR', NULL, NULL, NULL, NULL, 'SQL', '1', '2025-12-22T08:53:22.2539129', '2025-12-22 08:28:42.557', '2025-12-22 08:56:56.958 -0800', '14915191', '0', 'HEART_PARQUET_FORMAT', 'Y', 'SCANDS_PROD', 'dbo', 'F_CMSMMR', 'LOAD_DATETIME', '2025-12-22 08:28:42.557', 'FULL', false, NULL, 'BI-RPT-TEST', NULL, '1900-01-01', NULL, NULL, NULL, NULL, NULL),
    ('ff6430e2-38f0-4985-9f4f-66148a67542b', NULL, NULL, NULL, NULL, NULL, NULL, 'SQL', '1', '2026-01-21T10:18:59.9977290', '1900-01-01', '1900-01-01', '13352216', '0', 'HEART_PARQUET_FORMAT', NULL, 'BENEFIT', 'DBO', 'Benefit_Provider_Detail', NULL, NULL, 'FULL', false, NULL, 'AzureStagingPrd', NULL, '1900-01-01', NULL, NULL, NULL, NULL, NULL),
    ('ff16a0d9-0f01-4249-8a02-9487bcf271bb', 'MedHOK-CIL', 'MedHOK', 'Intervention_MEDICARE', 'Intervention_MEDICARE', 'csv', NULL, 'CIL-File', '1', '1900-01-01', '1900-01-01', '1900-01-01', '250', '0', 'HEART_MOCK_DDL_PIPE', 'Y', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '^(?:.*/)?Intervention_MEDICARE(?:_[0-9]+){3}\\.csv$'),
    ('ff16a0d9-0f01-4249-8a02-9487bcf271bb', NULL, 'medhok', 'Intervention_MEDICARE', 'Intervention_MEDICARE', 'csv', NULL, 'File', '1', '2026-01-03T12:03:49.6822158', '2026-01-03T12:04:19.0939536', '2026-01-03T12:04:19.0939947', '250', '0', 'HEART_MOCK_DDL_PIPE', 'Y', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
    ('ff13b82e-6740-4511-8451-04d4bd3801ac', NULL, NULL, NULL, NULL, NULL, NULL, 'SQL', '1', '1900-01-01', '2025-08-30 17:32:34.060', '2025-08-30 17:33:21.388 -0700', '0', '0', 'HEART_PARQUET_FORMAT', 'Y', 'SCANDS_Finance_Archive', '2025_Bid', 'dimMedicareFlag', 'LOAD_DATETIME', '2025-08-30 17:32:34.060', 'FULL', false, NULL, 'DSTREAM-TEST', NULL, '1900-01-01', NULL, NULL, NULL, NULL, NULL),
    ('fed44f3c-a30d-4787-8c49-9a2140ae56df', 'SupplementalBenefit Astrana', 'SupplementalBenefit', 'SCAN Accupuncture Report', 'Astrana', 'csv', NULL, 'File', '1', '2026-01-16T02:01:02.0111862', '2026-01-16T02:01:34.8025253', '2026-01-16T02:01:34.8025461', '727', '0', 'HEART_MOCK_DDL_PIPE_COMMA', 'Y', NULL, NULL, NULL, NULL, NULL, NULL, false, NULL, NULL, NULL, '1900-01-01', NULL, NULL, NULL, NULL, NULL),
    ('feb30ea1-2ee6-400a-8a8e-fc56fa631d70', NULL, NULL, NULL, NULL, NULL, NULL, 'SQL', '1', '2025-08-29T13:09:48.1856091', '2025-08-29 13:09:39.573', '2025-08-29 13:10:24.483 -0700', '3707', '0', 'HEART_PARQUET_FORMAT', 'Y', 'BIDProcess', 'dbo', 'dMRSpec', 'LOAD_DATETIME', '2025-08-29 13:09:39.573', 'FULL', false, NULL, 'BIMILL-APP-TEST', NULL, '1900-01-01', NULL, NULL, NULL, NULL, NULL),
    ('fe945663-a432-4a3b-ba76-85f2579a7f09', 'BIMILL-APP_BIDPROCESS', NULL, NULL, NULL, NULL, NULL, 'SQL', '1', '1900-01-01', '1900-01-01', '1900-01-01', 'N/A', '0', 'HEART_PARQUET_FORMAT', 'Y', 'BIDProcess', 'dbo', 'D_County', 'LOAD_DATETIME', NULL, 'FULL', false, NULL, 'BIMILL-APP-TEST', NULL, '1900-01-01', NULL, NULL, NULL, NULL, NULL),
    ('fe5f6424-71dd-4264-8354-8ab017f1b56a', NULL, 'medhok', 'Workflow_log_MEDICARE', 'Workflow_log_MEDICARE', 'csv', NULL, 'File', '1', '2026-01-03T11:58:29.3460964', '2026-01-03T11:58:56.0419382', '2026-01-03T11:58:56.0419680', '31780', '0', 'HEART_MOCK_DDL_PIPE', 'Y', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
    ('fe5f6424-71dd-4264-8354-8ab017f1b56a', 'MedHOK-CIL', 'MedHOK', 'Workflow_log_MEDICARE', 'Workflow_log_MEDICARE', 'csv', NULL, 'CIL-File', '1', '1900-01-01', '1900-01-01', '1900-01-01', '31780', '0', 'HEART_MOCK_DDL_PIPE', 'Y', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '^(?:.*/)?Workflow_log_MEDICARE(?:_[0-9]+){3}\\.csv$'),
    ('fe4ad6a5-5350-4e19-b19e-d269b2c1cb62', 'AgentCubedODS', 'AgentCubedODS', NULL, NULL, NULL, NULL, 'SQL', '1', '2026-01-21T02:10:26.7494962', '2026-01-21 02:10:19.797', '2026-01-21 02:11:35.720 -0800', '4923', '0', 'HEART_PARQUET_FORMAT', 'Y', 'AgentCubedODS', 'A3_curr', 'SeminarSingle', 'SysUpdateDateTime', '2025-10-16 20:58:21.133', 'INC', false, NULL, 'SHAREDSQL-TEST', NULL, '1900-01-01', NULL, NULL, NULL, NULL, NULL),
    ('fe32da3b-2bb9-4e41-93f4-64350737d69f', 'Path Azure SQL', NULL, NULL, NULL, NULL, NULL, 'Azure SQL', '1', '1900-01-01', '1900-01-01', '1900-01-01', '0', NULL, 'HEART_PARQUET_FORMAT', NULL, 'PATH_DEV', 'DBO', 'HealthPlan', 'CreatedDate', '1900-01-01', 'INC', false, NULL, 'uswtestpathsql.database.windows.net', NULL, '1900-01-01', NULL, NULL, NULL, NULL, NULL),
    ('fe2fc648-5171-4003-b3f4-e2fd0efa74b2', 'MEDHOK_DB', NULL, NULL, NULL, NULL, NULL, 'SQL', '1', '2025-12-08T14:57:12.8412322', '2025-12-08 14:57:06.257', '2025-12-08 14:58:08.502 -0800', '7293', '0', 'HEART_PARQUET_FORMAT', NULL, 'MedhokODS', 'mhk_curr', 'ServiceGridCategoryProcedure', 'LOAD_DATETIME', '2025-12-08 14:57:06.257', 'FULL', false, NULL, 'BI-RPT-TEST', NULL, '1900-01-01', NULL, NULL, NULL, NULL, NULL),
    ('fe2f4c6d-6baf-40da-88c3-ab8e6050c1eb', NULL, 'medhok', 'Module_Link_MEDICARE', 'Module_Link_MEDICARE', 'csv', NULL, 'File', '1', '2026-01-03T12:07:01.0853777', '2026-01-03T12:07:25.8956535', '2026-01-03T12:07:25.8956807', '60', '0', 'HEART_MOCK_DDL_PIPE', 'Y', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
    ('fe2f4c6d-6baf-40da-88c3-ab8e6050c1eb', 'MedHOK-CIL', 'MedHOK', 'Module_Link_MEDICARE', 'Module_Link_MEDICARE', 'csv', NULL, 'CIL-File', '1', '1900-01-01', '1900-01-01', '1900-01-01', '60', '0', 'HEART_MOCK_DDL_PIPE', 'Y', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '^(?:.*/)?Module_Link_MEDICARE(?:_[0-9]+){3}\\.csv$'),
    ('fdc0c959-4888-4676-8659-a7eb6c6ba0e5', 'MedHOK-CIL', 'MedHOK', 'MMR_MEDICARE', 'MMR_MEDICARE', 'csv', NULL, 'CIL-File', '1', '1900-01-01', '1900-01-01', '1900-01-01', '0', '0', 'HEART_MOCK_DDL_PIPE', 'Y', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '^(?:.*/)?MMR_MEDICARE(?:_[0-9]+){3}\\.csv$'),
    ('fdc0c959-4888-4676-8659-a7eb6c6ba0e5', NULL, 'medhok', 'MMR_MEDICARE', 'MMR_MEDICARE', 'csv', NULL, 'File', '1', '2026-01-03T12:03:47.5983373', '2026-01-03T12:04:16.2877240', '2026-01-03T12:04:16.2877502', '0', '0', 'HEART_MOCK_DDL_PIPE', 'Y', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
    ('fd87a1ef-a5fb-405a-afb1-0eed9e9f15b5', 'LEGACY_DATA - BI-RPT-TEST', NULL, NULL, NULL, NULL, NULL, 'SQL', '1', '2025-11-14T03:41:32.3870772', '2025-11-14 03:39:46.117', '2025-11-14 03:43:48.357 -0800', '5144591', '0', 'HEART_PARQUET_FORMAT', NULL, 'CLAIMS', 'dbo', 'AUDAUDITDETAIL', 'LOAD_DATETIME', '2025-11-14 03:39:46.117', 'FULL', false, NULL, 'BI-RPT-TEST', NULL, '1900-01-01', NULL, NULL, NULL, NULL, NULL),
    ('fd7ab6ef-e63e-4d23-a803-87c7e9a73bdd', 'NPPES', NULL, NULL, NULL, NULL, NULL, 'SQL', '1', '2025-12-17T12:07:55.4608396', '2025-12-17 12:07:48.583', '2025-12-17 12:08:24.323 -0800', '61', '0', 'HEART_PARQUET_FORMAT', 'N', 'HCIEncounters', 'dbo', 'nppes_state', 'LOAD_DATETIME', '2025-12-17 12:07:48.583', 'FULL', false, NULL, 'DVBI-TEST', NULL, '1900-01-01', NULL, NULL, NULL, NULL, NULL),
    ('fd497a02-267c-4eb0-bb4a-5fba3e63c70a', 'LEGACY', NULL, NULL, NULL, NULL, NULL, 'SQL', '1', '2025-12-09T06:13:12.9950369', '2025-12-09 06:13:02.317', '2025-12-09 06:13:42.301 -0800', '1952', '0', 'HEART_PARQUET_FORMAT', 'N', 'BIR_Objects', 'AEPOEP', 'rpt_AEPBudgetVsActual', 'LOAD_DATETIME', '2025-12-09 06:13:02.317', 'FULL', false, NULL, 'BI-RPT-PROD', NULL, '1900-01-01', NULL, NULL, NULL, NULL, NULL),
    ('fd2173fe-d9cc-4469-89d6-f0a725134526', NULL, NULL, NULL, NULL, NULL, NULL, 'SQL', '1', '1900-01-01', '2025-08-30 17:37:41.203', '2025-08-30 17:38:18.922 -0700', '0', '0', 'HEART_PARQUET_FORMAT', 'Y', 'DWOBJECTS', 'dbo', 'tbl_Fusion_Report42', 'LOAD_DATETIME', '2025-08-30 17:37:41.203', 'FULL', false, NULL, 'DSTREAM-TEST', NULL, '1900-01-01', NULL, NULL, NULL, NULL, NULL),
    ('fc35ad97-c233-436b-b4b7-eae7fad6e224', NULL, NULL, 'Genesys_Staging', NULL, NULL, NULL, 'csv', '1', '2025-09-22T05:11:42.4042370', '2025-09-22 05:11:34.827', '2025-09-22 05:12:29.903 -0700', '7', '0', 'HEART_PARQUET_FORMAT', NULL, 'GenesysODS', 'dbo', 'teams', 'SysInsertDateTime', '2025-09-22 04:00:21.430', 'FULL', false, NULL, 'SHAREDSQL-TEST', NULL, '1900-01-01', NULL, '0', NULL, NULL, NULL),
    ('fc33de5c-1bba-4019-a847-b731413e2c24', 'MedHOK-CIL', 'MedHOK', 'Labresults_MEDICARE', 'Labresults_MEDICARE', 'csv', NULL, 'CIL-File', '1', '1900-01-01', '1900-01-01', '1900-01-01', '5423', '0', 'HEART_MOCK_DDL_PIPE', 'Y', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '^(?:.*/)?Labresults_MEDICARE(?:_[0-9]+){3}\\.csv$'),
    ('fc33de5c-1bba-4019-a847-b731413e2c24', NULL, 'medhok', 'Labresults_MEDICARE', 'Labresults_MEDICARE', 'csv', NULL, 'File', '1', '2026-01-03T12:03:27.2254867', '2026-01-03T12:04:35.0319907', '2026-01-03T12:04:35.0320204', '5423', '0', 'HEART_MOCK_DDL_PIPE', 'Y', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
    ('fc30e0fe-0509-45e6-82b0-79e403054e01', NULL, NULL, NULL, NULL, NULL, NULL, 'SQL', '1', '2025-08-29T13:07:36.4057882', '1900-01-01', '1900-01-01', '0', '0', 'HEART_PARQUET_FORMAT', 'Y', 'BIDProcess', 'dbo', 'HIP_ckclm', 'LOAD_DATETIME', '1900-01-01', 'FULL', false, NULL, 'BIMILL-APP-TEST', NULL, '1900-01-01', NULL, NULL, NULL, NULL, NULL),
    ('fc1dee5c-a7da-4c0d-899a-2bd31a261080', NULL, NULL, NULL, NULL, NULL, NULL, 'SQL', '1', '2025-08-29T13:49:28.1894248', '1900-01-01', '1900-01-01', '0', '0', 'HEART_PARQUET_FORMAT', 'Y', 'BIDProcess', 'dbo', 'outClaims_Repricer_01222023', 'LOAD_DATETIME', '1900-01-01', 'FULL', false, NULL, 'BIMILL-APP-TEST', NULL, '1900-01-01', NULL, NULL, NULL, NULL, NULL),
    ('fbed8b02-4e6e-48e5-95ff-b7f5370ca9bc', NULL, 'ODSExtract', 'ODSExtract_ORAEXT_AP_Invoice_Distributions', 'AP_Invoice_Distributions', 'txt', NULL, 'CIL-File', '1', '1900-01-01', '1900-01-01', '1900-01-01', 'N/A', '0', 'HEART_MOCK_DDL_PIPE', 'Y', NULL, NULL, NULL, NULL, '1900-01-01', NULL, false, NULL, NULL, NULL, '1900-01-01', NULL, NULL, NULL, NULL, NULL),
    ('fb9fd51b-e8d7-4053-ad3c-1c788c7127cd', 'MARA_XPLN', 'Milliman_MARA', 'XPLN', 'ModelOutput_XPLN', 'csv', NULL, 'File', '1', '2025-11-28T02:18:33.5802243', '2025-11-28T02:18:55.6906458', '2025-11-28T02:18:55.6906692', '0', '0', 'HEART_MOCK_DDL_PIPE_COMMA', 'Y', NULL, NULL, NULL, NULL, NULL, NULL, false, NULL, NULL, NULL, '1900-01-01', NULL, NULL, NULL, NULL, NULL),
    ('fb9d2c2d-4734-4440-919d-b40026f071c4', 'LEGACY_DATA', NULL, NULL, NULL, NULL, NULL, 'SQL', '1', '1900-01-01', '2025-09-25 11:32:45.207', '2025-09-25 11:33:49.531 -0700', '0', NULL, 'HEART_PARQUET_FORMAT', 'Y', 'Care_Alert', 'dbo', 'member_chronic_conditions', 'LOAD_DATETIME', '2025-09-25 11:32:45.207', 'FULL', NULL, NULL, 'HCISYS-RPT-PROD', NULL, NULL, NULL, NULL, NULL, NULL, NULL),
    ('fb28db06-00cf-4633-ace7-ac709b40e4e8', NULL, NULL, 'Genesys_Staging', NULL, NULL, NULL, 'csv', '1', '2025-09-22T05:02:15.5709761', '2025-09-22 05:02:06.710', '2025-09-22 05:03:12.437 -0700', '309', '0', 'HEART_PARQUET_FORMAT', NULL, 'GenesysODS', 'dbo', 'allContactLists_Backup', 'SysInsertDateTime', '2025-09-21 04:01:27.120', 'FULL', false, NULL, 'SHAREDSQL-TEST', NULL, '1900-01-01', NULL, '0', NULL, NULL, NULL),
    ('fafbe32f-794b-4fa0-8925-49db5d076989', NULL, 'medhok', 'Careplan_Problems_MEDICARE', 'Careplan_Problems_MEDICARE', 'csv', NULL, 'File', '1', '2026-01-03T12:01:28.6027155', '2026-01-03T12:02:02.8469810', '2026-01-03T12:02:02.8470055', '473383', '0', 'HEART_MOCK_DDL_PIPE', 'Y', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
    ('fafbe32f-794b-4fa0-8925-49db5d076989', 'MedHOK-CIL', 'MedHOK', 'Careplan_Problems_MEDICARE', 'Careplan_Problems_MEDICARE', 'csv', NULL, 'CIL-File', '1', '1900-01-01', '1900-01-01', '1900-01-01', '473383', '0', 'HEART_MOCK_DDL_PIPE', 'Y', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '^(?:.*/)?Careplan_Problems_MEDICARE(?:_[0-9]+){3}\\.csv$'),
    ('fad0233b-6110-4309-a935-916f67b68624', NULL, 'CBdB', 'Premium_Rates_Rider', 'Premium_Rates_Rider', NULL, NULL, 'csv', '1', '2025-05-09T14:13:05.1902512', '2025-05-09T14:14:26.5037505', '2025-05-09T14:14:26.5037725', '9', '0', 'HEART_MOCK_DDL_PIPE_COMMA', 'Y', NULL, NULL, NULL, NULL, NULL, NULL, false, NULL, NULL, NULL, '1900-01-01', NULL, '0', NULL, NULL, NULL),
    ('faa2a8c4-ac8f-4574-a857-293ef464afb1', 'Path Azure SQL', NULL, NULL, NULL, NULL, NULL, 'Azure SQL', '1', '1900-01-01', '1900-01-01', '1900-01-01', '0', NULL, 'HEART_PARQUET_FORMAT', NULL, 'PATH_DEV', 'DBO', 'ProviderTerritory', 'CreatedDate', '1900-01-01', 'INC', false, NULL, 'uswtestpathsql.database.windows.net', NULL, '1900-01-01', NULL, NULL, NULL, NULL, NULL),
    ('fa8467a3-3c85-4312-b202-d461e0be0a80', NULL, NULL, NULL, NULL, NULL, NULL, 'csv', '1', '1900-01-01', '2026-01-05 22:56:31.857', '2026-01-05 22:57:37.699 -0800', 'N/A', '0', 'HEART_PARQUET_FORMAT', 'Y', 'MDS', 'dbo', 'Geocoded_Address', 'Creation_Date', '2026-01-05 22:56:31.857', 'INC', false, NULL, 'BI-RPT-TEST', NULL, '1900-01-01', NULL, NULL, NULL, NULL, NULL),
    ('f9e6d0d0-7c75-448c-b375-d83b944ada8e', 'LEGACY_DATA', NULL, NULL, NULL, NULL, NULL, 'SQL', '1', '2025-11-06T23:43:42.0810244', '2025-11-06 23:43:28.970', '2025-11-06 23:45:04.989 -0800', '420879', '0', 'HEART_PARQUET_FORMAT', NULL, 'prod', 'dbo', 'SD_AthenaBaseReport', 'LOAD_DATETIME', '2025-11-06 23:43:28.970', 'FULL', false, NULL, 'hcira-datamart', NULL, '1900-01-01', NULL, NULL, NULL, NULL, NULL),
    ('f9b26699-6309-4d3e-8a85-c53fa74ea7c9', NULL, 'Milliman', NULL, NULL, NULL, NULL, 'SQL', '1', '2025-12-23T07:16:23.0815090', '1900-01-01', '1900-01-01', '0', '0', 'HEART_PARQUET_FORMAT', 'Y', 'SCANDS_STAGING', 'dbo', 'Mara_CxXPLN_InvalidClaim', 'LOAD_DATETIME', '1900-01-01', 'FULL', false, NULL, 'DS-STAGE-TEST', NULL, NULL, NULL, NULL, NULL, NULL, NULL),
    ('f962bb29-e633-4524-ac31-94b00b0ab3bd', 'NPPES', NULL, NULL, NULL, NULL, NULL, 'SQL', '1', '2025-12-17T12:03:53.8340737', '2025-12-17 12:03:45.693', '2025-12-17 12:05:09.152 -0800', '6', '0', 'HEART_PARQUET_FORMAT', 'N', 'HCIEncounters', 'dbo', 'nppes_other_name_type', 'LOAD_DATETIME', '2025-12-17 12:03:45.693', 'FULL', false, NULL, 'DVBI-TEST', NULL, '1900-01-01', NULL, NULL, NULL, NULL, NULL),
    ('f94eb0d5-4526-4584-89b4-7e9142175cae', NULL, NULL, NULL, NULL, NULL, NULL, 'SQL', '1', '1900-01-01', '2025-08-30 16:57:42.207', '2025-08-30 16:58:33.230 -0700', '0', '0', 'HEART_PARQUET_FORMAT', 'Y', 'SCANDS_Finance_Archive', '2024_BID', 'DimServiceDate', 'LOAD_DATETIME', '2025-08-30 16:57:42.207', 'FULL', false, NULL, 'DSTREAM-TEST', NULL, '1900-01-01', NULL, NULL, NULL, NULL, NULL),
    ('f9413755-627e-4e0d-a554-a21c363fb4fb', 'Path Azure SQL', NULL, NULL, NULL, NULL, NULL, 'Azure SQL', '1', '1900-01-01', '1900-01-01', '1900-01-01', '0', '0', 'HEART_PARQUET_FORMAT', 'Y', 'PATH_DEV', 'dbo', 'OrgAppointmentTypeMapping', 'LOAD_DATETIME', '1900-01-01', 'FULL', false, NULL, 'uswtestpathsql.database.windows.net', NULL, '1900-01-01', NULL, NULL, NULL, NULL, NULL);
""",
    },
    {
        "name": "METADATA_SQL_SERVER_LOOKUP",
        "description": "Mock SQL Server metadata rows for table lookup filters",
        "ddl": """
CREATE OR REPLACE TABLE {table_ref} (
    LOGICAL_NAME VARCHAR(16777216),
    SERVER_NAME VARCHAR(255),
    DATABASE_NAME VARCHAR(255),
    SCHEMA_NAME VARCHAR(255),
    SOURCE_TABLE_NAME VARCHAR(255),
    SOURCE_TYPE VARCHAR(50)
);
""",
        "dml": """
INSERT INTO {table_ref} (
    LOGICAL_NAME, SERVER_NAME, DATABASE_NAME, SCHEMA_NAME, SOURCE_TABLE_NAME, SOURCE_TYPE
) VALUES
    ('Claims Core', 'SHAREDSQL-TEST', 'CLAIMS', 'dbo', 'ClaimHeader', 'SQL_SERVER'),
    ('Claims Core', 'SHAREDSQL-TEST', 'CLAIMS', 'dbo', 'ClaimLine', 'SQL_SERVER'),
    ('Contact Center', 'SCAN-CX-TEST', 'GenesysODS', 'dbo', 'Calls', 'SQL_SERVER'),
    ('Provider Network', 'BI-RPT-TEST', 'ProviderHub', 'dbo', 'Provider', 'SQL_SERVER'),
    ('Enrollment', 'EDI-Test01', 'ElectronicEnrollment', 'dbo', 'MemberEnrollment', 'SQL_SERVER');
""",
    },
    {
        "name": "METADATA_CONFIG_TABLE_ELT_ADHOC",
        "description": "Adhoc metadata configuration table for on-demand ELT triggers",
        "date_column": "LAST_TRIGGER_TIMESTAMP",
        "ddl": """
CREATE OR REPLACE TABLE {table_ref} (
    METADATA_CONFIG_KEY VARCHAR(16777216),
    LOGICAL_NAME VARCHAR(16777216),
    DATABASE_NAME VARCHAR(255),
    SCHEMA_NAME VARCHAR(255),
    SOURCE_TABLE_NAME VARCHAR(255),
    LOAD_START_DATETIME TIMESTAMP,
    LOAD_END_DATETIME TIMESTAMP,
    LAST_TRIGGER_TIMESTAMP TIMESTAMP,
    ERROR_STATUS VARCHAR(255),
    PAYLOAD VARIANT
);
""",
        "dml": """
INSERT INTO {table_ref} (
    METADATA_CONFIG_KEY, LOGICAL_NAME, DATABASE_NAME, SCHEMA_NAME, SOURCE_TABLE_NAME,
    LOAD_START_DATETIME, LOAD_END_DATETIME, LAST_TRIGGER_TIMESTAMP, ERROR_STATUS
) VALUES
    ('adhoc-001', 'AgentCubedODS', 'AgentCubedODS', 'A3_CURR', 'AgencyManagementParty', '2026-01-27 08:00:00', '2026-01-27 08:15:00', '2026-01-27 07:55:00', 'COMPLETED'),
    ('adhoc-002', 'MindfulODS', 'MindfulODS', 'dbo', 'Interactions', '2026-01-27 09:00:00', '2026-01-27 09:10:00', '2026-01-27 08:55:00', 'COMPLETED'),
    ('adhoc-003', 'GenesysODS', 'GenesysODS', 'dbo', 'CallRecords', '2026-01-28 06:00:00', NULL, '2026-01-28 05:55:00', 'TRIGGERED_NOT_STARTED'),
    ('adhoc-004', 'LEGACY', 'SCANDS_PROD', 'dbo', 'D_Site_Attributes', '2026-01-28 10:00:00', '2026-01-28 10:22:00', '2026-01-28 09:58:00', 'COMPLETED'),
    ('adhoc-005', 'LEGACY', 'SCANDS_STAGING', 'dbo', 'CMSLTIValue', '2026-01-28 11:00:00', '2026-01-28 11:05:00', '2026-01-28 10:55:00', 'COMPLETED'),
    ('adhoc-006', 'Excel_Test_CSV', 'SANDBOX', 'dbo', 'CareLinx_Member_Utilization_CSV', '2026-01-28 12:00:00', NULL, '2026-01-28 11:58:00', 'ERROR'),
    ('adhoc-007', 'MedHOK-CIL', 'MedHOK', 'dbo', 'Intervention_MEDICARE', '2026-01-28 13:00:00', '2026-01-28 13:30:00', '2026-01-28 12:55:00', 'COMPLETED'),
    ('adhoc-008', 'SupplementalBenefit', 'SupplementalBenefit', 'dbo', 'Astrana', '2026-01-28 14:00:00', NULL, '2026-01-28 13:58:00', 'TRIGGERED_NOT_STARTED'),
    ('adhoc-009', 'AgentCubedODS', 'AgentCubedODS', 'A3_CURR', 'AgentProfile', '2026-01-28 15:00:00', '2026-01-28 15:45:00', '2026-01-28 14:55:00', 'COMPLETED'),
    ('adhoc-010', 'GenesysODS', 'GenesysODS', 'dbo', 'AgentActivity', '2026-01-28 16:00:00', '2026-01-28 16:20:00', '2026-01-28 15:58:00', 'COMPLETED'),
    ('adhoc-011', 'MindfulODS', 'MindfulODS', 'dbo', 'SurveyResponses', '2026-01-29 06:00:00', NULL, '2026-01-29 05:55:00', 'TRIGGERED_NOT_STARTED'),
    ('adhoc-012', 'LEGACY', 'SCANDS_Finance', 'dbo', 'F_ClaimPayment', '2026-01-29 07:00:00', '2026-01-29 07:35:00', '2026-01-29 06:58:00', 'COMPLETED'),
    ('adhoc-013', 'AgentCubedODS', 'AgentCubedODS', 'A3_CURR', 'PolicyHolder', '2026-01-29 08:00:00', NULL, '2026-01-29 07:55:00', 'FAILED'),
    ('adhoc-014', 'LEGACY', 'SCANDS_PROD', 'dbo', 'B_MemberApplicationHistory', '2026-01-29 09:00:00', '2026-01-29 09:50:00', '2026-01-29 08:58:00', 'COMPLETED'),
    ('adhoc-015', 'GenesysODS', 'GenesysODS', 'dbo', 'QueueMetrics', '2026-01-29 10:00:00', NULL, '2026-01-29 09:58:00', 'TRIGGERED_NOT_STARTED');
""",
    },
    {
        "name": "METADATA_CONFIG_TABLE_ELT_ADHOC_VW",
        "description": "View wrapper for adhoc metadata config table (DuckDB compatibility)",
        "ddl": """
CREATE OR REPLACE VIEW {table_ref} AS
SELECT
    METADATA_CONFIG_KEY,
    LOGICAL_NAME,
    DATABASE_NAME,
    SCHEMA_NAME,
    SOURCE_TABLE_NAME,
    LOAD_START_DATETIME,
    LOAD_END_DATETIME,
    LAST_TRIGGER_TIMESTAMP,
    ERROR_STATUS,
    PAYLOAD,
    'LOCAL' AS ENVIRONMENT
FROM METADATA_CONFIG_TABLE_ELT_ADHOC;
""",
    },
    {
        "name": "METADATA_SQL_SERVER_LOOKUP_VW",
        "description": "View for SQL Server table lookup filters",
        "ddl": """
CREATE OR REPLACE VIEW {table_ref} AS
SELECT
    LOGICAL_NAME,
    SERVER_NAME,
    DATABASE_NAME,
    SCHEMA_NAME,
    SOURCE_TABLE_NAME,
    SOURCE_TYPE
FROM {schema_prefix}METADATA_SQL_SERVER_LOOKUP
WHERE UPPER(SOURCE_TYPE) = 'SQL_SERVER'
ORDER BY LOGICAL_NAME, SERVER_NAME, DATABASE_NAME, SCHEMA_NAME, SOURCE_TABLE_NAME;
""",
        "dml": "",
    },
    {
        "name": "METADATA_SQL_SOURCES_VW",
        "description": "Distinct SQL sources with display label for ingestion copilot",
        "ddl": """
CREATE OR REPLACE VIEW {table_ref} AS
SELECT DISTINCT
    LOGICAL_NAME,
    DATABASE_NAME,
    SCHEMA_NAME,
    CONCAT(LOGICAL_NAME, ' | ', DATABASE_NAME, '.', SCHEMA_NAME) AS SOURCE_LABEL
FROM {schema_prefix}METADATA_CONFIG_TABLE_ELT
WHERE UPPER(SOURCE_TYPE) = 'SQL'
  AND LOGICAL_NAME IS NOT NULL
  AND DATABASE_NAME IS NOT NULL
  AND SCHEMA_NAME IS NOT NULL
ORDER BY LOGICAL_NAME, DATABASE_NAME, SCHEMA_NAME;
""",
        "dml": "",
    },
    {
        "name": "METADATA_SQL_SOURCES_PICKLIST_VW",
        "description": "Distinct SQL sources with server for picklists",
        "ddl": """
CREATE OR REPLACE VIEW {table_ref} AS
SELECT DISTINCT
        LOGICAL_NAME,
        DATABASE_NAME,
        SCHEMA_NAME,
        SERVER_NAME,
        CONCAT(COALESCE(SERVER_NAME, 'Unknown'), '.', DATABASE_NAME, '.', SCHEMA_NAME, ' - (', LOGICAL_NAME, ')') AS SOURCE_LABEL
FROM {schema_prefix}METADATA_CONFIG_TABLE_ELT
WHERE UPPER(SOURCE_TYPE) = 'SQL'
  AND LOGICAL_NAME IS NOT NULL
  AND DATABASE_NAME IS NOT NULL
    AND SCHEMA_NAME IS NOT NULL
ORDER BY DATABASE_NAME, SCHEMA_NAME, SERVER_NAME, LOGICAL_NAME;
""",
        "dml": "",
    },
    {
        "name": "METADATA_SQL_TABLES_VW",
        "description": "All SQL metadata rows with fully-qualified source table",
        "ddl": """
CREATE OR REPLACE VIEW {table_ref} AS
SELECT
    t.*,
    CONCAT(t.DATABASE_NAME, '.', t.SCHEMA_NAME, '.', t.SOURCE_TABLE_NAME) AS QUALIFIED_SOURCE_TABLE
FROM {schema_prefix}METADATA_CONFIG_TABLE_ELT AS t
WHERE UPPER(t.SOURCE_TYPE) = 'SQL';
""",
        "dml": "",
    },
    {
        "name": "METADATA_SQL_DELTA_COLUMNS_VW",
        "description": "Distinct delta columns per SQL source dimensions",
        "ddl": """
CREATE OR REPLACE VIEW {table_ref} AS
SELECT DISTINCT
    LOGICAL_NAME,
    DATABASE_NAME,
    SCHEMA_NAME,
    SERVER_NAME,
    DELTA_COLUMN
FROM {schema_prefix}METADATA_CONFIG_TABLE_ELT
WHERE UPPER(SOURCE_TYPE) = 'SQL'
  AND LOGICAL_NAME IS NOT NULL
  AND DATABASE_NAME IS NOT NULL
  AND SCHEMA_NAME IS NOT NULL
  AND DELTA_COLUMN IS NOT NULL;
""",
        "dml": "",
    },
        {
                "name": "METADATA_SQL_CHANGE_TRACKING_VW",
                "description": "Distinct change tracking types per SQL source dimensions",
                "ddl": """
CREATE OR REPLACE VIEW {table_ref} AS
SELECT DISTINCT
        LOGICAL_NAME,
        DATABASE_NAME,
        SCHEMA_NAME,
        SERVER_NAME,
        CHANGE_TRACKING_TYPE
FROM {schema_prefix}METADATA_CONFIG_TABLE_ELT
WHERE UPPER(SOURCE_TYPE) = 'SQL'
    AND LOGICAL_NAME IS NOT NULL
    AND DATABASE_NAME IS NOT NULL
    AND SCHEMA_NAME IS NOT NULL
    AND CHANGE_TRACKING_TYPE IS NOT NULL;
""",
                "dml": "",
        },
    {
        "name": "ELT_JOB_RUN",
        "description": "Job run history for ingestion pipelines",
        "date_column": "START_TIME",
        "ddl": """
CREATE OR REPLACE TABLE {table_ref} (
    RUN_ID VARCHAR(100),
    START_TIME TIMESTAMP_NTZ(9),
    END_TIME TIMESTAMP_NTZ(9),
    STATUS VARCHAR(50),
    ROWS_PROCESSED NUMBER(38,0),
    ERROR_COUNT NUMBER(38,0) DEFAULT 0,
    EXECUTION_TIME NUMBER(38,0),
    TRIGGER_SOURCE VARCHAR(255)
);
""",
        "dml": """
INSERT INTO {table_ref} (
    RUN_ID, START_TIME, END_TIME, STATUS, ROWS_PROCESSED, ERROR_COUNT, EXECUTION_TIME, TRIGGER_SOURCE
) VALUES
    ('209216ce-38b2-4d70-bc9b-c78db2e14ba4', '2026-01-22 15:45:01.536', '2026-01-22 07:47:53.175', 'SUCCEDED', 0, 0, -28628, 'ScheduleTrigger'),
    ('118c9ee1-29af-4832-8185-0c9d6f27eddc', '2026-01-22 13:45:01.177', '2026-01-22 05:48:37.523', 'SUCCEDED', 205, 0, -28584, 'ScheduleTrigger'),
    ('20a53c60-3150-4a27-8014-8cc83f1fe299', '2026-01-22 12:45:01.014', '2026-01-22 04:48:10.066', 'SUCCEDED', 0, 0, -28611, 'ScheduleTrigger'),
    ('bbde76cb-46c2-4e50-aa5e-b17c6cd716a7', '2026-01-22 06:45:00.596', '2026-01-21 22:47:50.877', 'SUCCEDED', 907, 0, -28630, 'ScheduleTrigger'),
    ('2df4627e-7962-489e-a242-dfbde3542de6', '2026-01-22 03:19:47.142', '2026-01-21 19:29:34.526', 'SUCCEDED', 1655, 0, -28213, 'Manual'),
    ('2df4627e-7962-489e-a242-dfbde3542de6', '2026-01-22 03:19:47.142', '2026-01-21 19:27:57.464', 'SUCCEDED', 5, 0, -28310, 'Manual'),
    ('2df4627e-7962-489e-a242-dfbde3542de6', '2026-01-22 03:19:47.142', '2026-01-21 19:22:31.707', 'SUCCEDED', 178, 0, -28636, 'Manual'),
    ('68c60ff5-b7fb-4182-8721-2e7980086e0e', '2026-01-22 02:57:51.070', '2026-01-21 19:07:06.970', 'SUCCEDED', 0, 0, -28245, 'Manual'),
    ('68c60ff5-b7fb-4182-8721-2e7980086e0e', '2026-01-22 02:57:51.070', '2026-01-21 19:07:18.708', 'SUCCEDED', 0, 0, -28233, 'Manual'),
    ('68c60ff5-b7fb-4182-8721-2e7980086e0e', '2026-01-22 02:57:51.070', '2026-01-21 19:01:48.783', 'SUCCEDED', 0, 0, -28563, 'Manual');
""",
    },
    {
        "name": "ELT_JOB_RUN_METRICS_VW",
        "description": "Aggregated job run metrics by trigger and status",
        "ddl": """
CREATE OR REPLACE VIEW {table_ref} AS
SELECT
    TRIGGER_SOURCE,
    STATUS,
    COUNT(*) AS RUNS,
    SUM(ROWS_PROCESSED) AS TOTAL_ROWS_PROCESSED,
    SUM(ERROR_COUNT) AS TOTAL_ERRORS,
    AVG(EXECUTION_TIME) AS AVG_EXECUTION_TIME,
    MAX(EXECUTION_TIME) AS MAX_EXECUTION_TIME,
    MIN(EXECUTION_TIME) AS MIN_EXECUTION_TIME,
    MAX(START_TIME) AS LAST_START_TIME,
    MAX(END_TIME) AS LAST_END_TIME
FROM {schema_prefix}ELT_JOB_RUN
GROUP BY TRIGGER_SOURCE, STATUS
ORDER BY TRIGGER_SOURCE, STATUS;
""",
        "dml": "",
    },
    {
        "name": "ELT_JOB_RUN_DAILY_VW",
        "description": "Daily rollup of job runs by trigger and status",
        "ddl": """
CREATE OR REPLACE VIEW {table_ref} AS
SELECT
    DATE(START_TIME) AS RUN_DATE,
    TRIGGER_SOURCE,
    STATUS,
    COUNT(*) AS RUNS,
    SUM(ROWS_PROCESSED) AS TOTAL_ROWS,
    SUM(ERROR_COUNT) AS TOTAL_ERRORS,
    AVG(EXECUTION_TIME) AS AVG_EXECUTION_TIME,
    MAX(EXECUTION_TIME) AS MAX_EXECUTION_TIME,
    MIN(EXECUTION_TIME) AS MIN_EXECUTION_TIME
FROM {schema_prefix}ELT_JOB_RUN
GROUP BY RUN_DATE, TRIGGER_SOURCE, STATUS
ORDER BY RUN_DATE DESC, TRIGGER_SOURCE, STATUS;
""",
        "dml": "",
    },
    {
        "name": "ELT_JOB_RUN_LAST_RUN_VW",
        "description": "Last job run per trigger source with status and timings",
        "ddl": """
CREATE OR REPLACE VIEW {table_ref} AS
SELECT
    TRIGGER_SOURCE,
    MAX(START_TIME) AS LAST_START_TIME,
    ANY_VALUE(END_TIME) AS LAST_END_TIME,
    ANY_VALUE(STATUS) AS LAST_STATUS,
    ANY_VALUE(ROWS_PROCESSED) AS LAST_ROWS_PROCESSED,
    ANY_VALUE(ERROR_COUNT) AS LAST_ERROR_COUNT,
    ANY_VALUE(EXECUTION_TIME) AS LAST_EXECUTION_TIME,
    ANY_VALUE(RUN_ID) AS LAST_RUN_ID
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY TRIGGER_SOURCE ORDER BY START_TIME DESC) AS RN
    FROM {schema_prefix}ELT_JOB_RUN
) t
WHERE RN = 1
GROUP BY TRIGGER_SOURCE
ORDER BY TRIGGER_SOURCE;
""",
        "dml": "",
    },
    {
        "name": "ELT_LOAD_LOG",
        "description": "Load log entries for ingestion loads",
        "date_column": "LOAD_START_TIME",
        "ddl": """
CREATE OR REPLACE TABLE {table_ref} (
    LOAD_ID VARCHAR(100),
    RUN_ID VARCHAR(100),
    LOAD_NAME VARCHAR(100),
    LOAD_START_TIME TIMESTAMP_NTZ(9),
    LOAD_END_TIME TIMESTAMP_NTZ(9),
    ROWS_LOADED NUMBER(38,0),
    ERROR_COUNT NUMBER(38,0) DEFAULT 0
);
""",
        "dml": """
INSERT INTO {table_ref} (
    LOAD_ID, RUN_ID, LOAD_NAME, LOAD_START_TIME, LOAD_END_TIME, ROWS_LOADED, ERROR_COUNT
) VALUES
    ('a1', 'run-001', 'LOAD_MEMBERS', '2026-01-20 10:00:00', '2026-01-20 10:05:30', 12000, 0),
    ('a2', 'run-002', 'LOAD_MEMBERS', '2026-01-21 10:00:00', '2026-01-21 10:06:10', 0, 2),
    ('a3', 'run-003', 'LOAD_MEMBERS', '2026-01-22 10:00:00', '2026-01-22 10:07:05', 11800, 0),
    ('b1', 'run-101', 'LOAD_CLAIMS', '2026-01-21 12:15:00', '2026-01-21 12:22:00', 45000, 0),
    ('c1', 'run-201', 'LOAD_PROVIDERS', '2026-01-22 08:05:00', '2026-01-22 08:14:00', 5200, 1),
    ('d1', 'run-301', 'LOAD_SURVEYS', '2026-01-19 09:30:00', '2026-01-19 09:36:20', 6400, 0);
""",
    },
    {
        "name": "ELT_LOAD_LOG_SUMMARY_VW",
        "description": "Aggregated load log metrics by load name",
        "ddl": """
CREATE OR REPLACE VIEW {table_ref} AS
SELECT
    LOAD_NAME,
    COUNT(*) AS RUNS,
    SUM(ROWS_LOADED) AS TOTAL_ROWS,
    SUM(ERROR_COUNT) AS TOTAL_ERRORS,
    AVG(DATEDIFF('second', LOAD_START_TIME, LOAD_END_TIME)) AS AVG_DURATION_SEC,
    MAX(LOAD_START_TIME) AS LAST_START_TIME,
    MAX(LOAD_END_TIME) AS LAST_END_TIME
FROM {schema_prefix}ELT_LOAD_LOG
GROUP BY LOAD_NAME
ORDER BY LOAD_NAME;
""",
        "dml": "",
    },
    {
        "name": "ELT_LOAD_LOG_ERROR_DAILY_VW",
        "description": "Daily error counts by load and day",
        "ddl": """
CREATE OR REPLACE VIEW {table_ref} AS
SELECT
    DATE_TRUNC('DAY', LOAD_START_TIME) AS RUN_DATE,
    LOAD_NAME,
    SUM(ERROR_COUNT) AS TOTAL_ERRORS,
    COUNT(*) AS RUNS_WITH_ERRORS
FROM {schema_prefix}ELT_LOAD_LOG
WHERE ERROR_COUNT <> 0
GROUP BY RUN_DATE, LOAD_NAME
ORDER BY RUN_DATE DESC, LOAD_NAME;
""",
        "dml": "",
    },
    {
        "name": "ELT_LOAD_LOG_LAST_RUN_VW",
        "description": "Last load run per load name with duration",
        "ddl": """
CREATE OR REPLACE VIEW {table_ref} AS
SELECT
    LOAD_NAME,
    LOAD_ID,
    RUN_ID,
    LOAD_START_TIME AS LAST_START_TIME,
    LOAD_END_TIME AS LAST_END_TIME,
    ROWS_LOADED AS LAST_ROWS_LOADED,
    ERROR_COUNT AS LAST_ERROR_COUNT,
    DATEDIFF('second', LOAD_START_TIME, LOAD_END_TIME) AS LAST_DURATION_SEC
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY LOAD_NAME ORDER BY LOAD_START_TIME DESC) AS RN
    FROM {schema_prefix}ELT_LOAD_LOG
) t
WHERE RN = 1
ORDER BY LOAD_NAME;
""",
        "dml": "",
    },
    {
        "name": "ELT_JOB_RUN_ERROR",
        "description": "Job run errors captured with timestamps and stages",
        "date_column": "ERROR_TIMESTAMP",
        "ddl": """
CREATE OR REPLACE TABLE {table_ref} (
    ERROR_ID VARCHAR(100),
    RUN_ID VARCHAR(100),
    ERROR_TIMESTAMP TIMESTAMP_NTZ(9),
    ERROR_MESSAGE VARCHAR(16777216),
    SOURCE_STAGE VARCHAR(255)
);
""",
        "dml": """
INSERT INTO {table_ref} (
    ERROR_ID, RUN_ID, ERROR_TIMESTAMP, ERROR_MESSAGE, SOURCE_STAGE
) VALUES
    ('err-001', 'run-002', '2026-01-21 10:06:30', 'Java Runtime Environment cannot be found on the Self-hosted Integration Runtime machine.', 'Create_Snowflake_Objects'),
    ('err-002', 'run-201', '2026-01-22 08:06:10', 'SQL compilation error: invalid identifier SUCCEEDED', 'Create_Snowflake_Objects'),
    ('err-003', 'run-201', '2026-01-22 08:07:40', 'Cannot connect to SQL Database. Login failed for user.', 'Create_Snowflake_Objects'),
    ('err-004', 'run-101', '2026-01-21 12:18:30', 'OutOfMemoryException during parquet write', 'Load_Claims'),
    ('err-005', 'run-003', '2026-01-22 10:03:10', 'Java Native Interface error: Cannot create JVM', 'Load_Members'),
    ('err-006', 'run-301', '2026-01-19 09:32:10', 'SQL compilation error: syntax error near unexpected token', 'Load_Surveys');
""",
    },
    {
        "name": "ELT_ERROR_LOG_DAILY_VW",
        "description": "Daily error counts by source stage",
        "ddl": """
CREATE OR REPLACE VIEW {table_ref} AS
SELECT
    DATE_TRUNC('DAY', ERROR_TIMESTAMP) AS ERROR_DATE,
    SOURCE_STAGE,
    COUNT(*) AS TOTAL_ERRORS
FROM {schema_prefix}ELT_JOB_RUN_ERROR
GROUP BY ERROR_DATE, SOURCE_STAGE
ORDER BY ERROR_DATE DESC, SOURCE_STAGE;
""",
        "dml": "",
    },
    {
        "name": "ELT_ERROR_LOG_TOP_MESSAGES_VW",
        "description": "Most frequent error messages per source stage",
        "ddl": """
CREATE OR REPLACE VIEW {table_ref} AS
SELECT
    SOURCE_STAGE,
    ERROR_MESSAGE,
    COUNT(*) AS OCCURRENCES,
    MAX(ERROR_TIMESTAMP) AS LAST_SEEN
FROM {schema_prefix}ELT_JOB_RUN_ERROR
GROUP BY SOURCE_STAGE, ERROR_MESSAGE
QUALIFY ROW_NUMBER() OVER (PARTITION BY SOURCE_STAGE ORDER BY OCCURRENCES DESC, LAST_SEEN DESC) <= 5
ORDER BY SOURCE_STAGE, OCCURRENCES DESC;
""",
        "dml": "",
    },
    {
        "name": "ELT_ERROR_LOG_LAST_VW",
        "description": "Most recent error per source stage",
        "ddl": """
CREATE OR REPLACE VIEW {table_ref} AS
SELECT
    SOURCE_STAGE,
    ERROR_ID,
    RUN_ID,
    ERROR_TIMESTAMP AS LAST_ERROR_TIMESTAMP,
    ERROR_MESSAGE AS LAST_ERROR_MESSAGE
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY SOURCE_STAGE ORDER BY ERROR_TIMESTAMP DESC) AS RN
    FROM {schema_prefix}ELT_JOB_RUN_ERROR
) t
WHERE RN = 1
ORDER BY SOURCE_STAGE;
""",
        "dml": "",
    },
    {
        "name": "ELT_LOAD_COMPARISON",
        "description": "Per-table source vs target row counts by environment",
        "date_column": "RUN_DATE",
        "ddl": """
CREATE OR REPLACE TABLE {table_ref} (
    ENVIRONMENT VARCHAR(10),
    SOURCE_NAME VARCHAR(255),
    DATABASE_NAME VARCHAR(255),
    SCHEMA_NAME VARCHAR(255),
    TABLE_NAME VARCHAR(255),
    TABLE_TYPE VARCHAR(50),
    RUN_DATE DATE,
    SOURCE_ROW_COUNT NUMBER(38,0),
    TARGET_ROW_COUNT NUMBER(38,0),
    LAST_REFRESH_TIME TIMESTAMP_NTZ(9)
);
""",
        "dml": """
INSERT INTO {table_ref} (
    ENVIRONMENT, SOURCE_NAME, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, TABLE_TYPE, RUN_DATE, SOURCE_ROW_COUNT, TARGET_ROW_COUNT, LAST_REFRESH_TIME
) VALUES
    ('DEV', 'CRM', 'STAGE_DEV', 'ELT', 'FACT_INTERACTIONS', 'FACT', '2026-01-22', 102345, 102340, '2026-01-22 06:10:00'),
    ('DEV', 'CRM', 'STAGE_DEV', 'ELT', 'DIM_AGENT', 'DIM', '2026-01-22', 1234, 1234, '2026-01-22 06:15:00'),
    ('TEST', 'CRM', 'STAGE_TEST', 'ELT', 'FACT_INTERACTIONS', 'FACT', '2026-01-22', 99876, 99870, '2026-01-22 05:50:00'),
    ('TEST', 'CRM', 'STAGE_TEST', 'ELT', 'DIM_AGENT', 'DIM', '2026-01-22', 1200, 1198, '2026-01-22 05:55:00'),
    ('UAT', 'CRM', 'STAGE_UAT', 'ELT', 'FACT_INTERACTIONS', 'FACT', '2026-01-21', 101500, 101500, '2026-01-21 04:40:00'),
    ('PROD', 'CRM', 'STAGE_PROD', 'ELT', 'FACT_INTERACTIONS', 'FACT', '2026-01-22', 150234, 150230, '2026-01-22 02:20:00'),
    ('PROD', 'CRM', 'STAGE_PROD', 'ELT', 'DIM_AGENT', 'DIM', '2026-01-22', 2500, 2498, '2026-01-22 02:25:00'),
    ('DEV', 'BILLING', 'STAGE_DEV', 'ELT', 'RAW_CLAIMS', 'RAW', '2026-01-22', 502000, 501990, '2026-01-22 03:15:00'),
    ('TEST', 'BILLING', 'STAGE_TEST', 'ELT', 'RAW_CLAIMS', 'RAW', '2026-01-21', 480500, 480400, '2026-01-21 03:18:00'),
    ('UAT', 'BILLING', 'STAGE_UAT', 'ELT', 'RAW_CLAIMS', 'RAW', '2026-01-22', 510000, 509950, '2026-01-22 01:55:00'),
    ('PROD', 'BILLING', 'STAGE_PROD', 'ELT', 'RAW_CLAIMS', 'RAW', '2026-01-22', 750000, 749990, '2026-01-22 01:10:00');
""",
    },
    {
        "name": "ELT_LOAD_COMPARISON_ENV_VW",
        "description": "Environment-tagged union view for load comparison",
        "ddl": """
CREATE OR REPLACE VIEW {table_ref} AS
SELECT 'DEV' AS ENVIRONMENT, SOURCE_NAME, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, TABLE_TYPE, RUN_DATE, SOURCE_ROW_COUNT, TARGET_ROW_COUNT, LAST_REFRESH_TIME
FROM {schema_prefix}ELT_LOAD_COMPARISON
WHERE UPPER(ENVIRONMENT) = 'DEV'
UNION ALL
SELECT 'TEST' AS ENVIRONMENT, SOURCE_NAME, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, TABLE_TYPE, RUN_DATE, SOURCE_ROW_COUNT, TARGET_ROW_COUNT, LAST_REFRESH_TIME
FROM {schema_prefix}ELT_LOAD_COMPARISON
WHERE UPPER(ENVIRONMENT) = 'TEST'
UNION ALL
SELECT 'UAT' AS ENVIRONMENT, SOURCE_NAME, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, TABLE_TYPE, RUN_DATE, SOURCE_ROW_COUNT, TARGET_ROW_COUNT, LAST_REFRESH_TIME
FROM {schema_prefix}ELT_LOAD_COMPARISON
WHERE UPPER(ENVIRONMENT) = 'UAT'
UNION ALL
SELECT 'PROD' AS ENVIRONMENT, SOURCE_NAME, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, TABLE_TYPE, RUN_DATE, SOURCE_ROW_COUNT, TARGET_ROW_COUNT, LAST_REFRESH_TIME
FROM {schema_prefix}ELT_LOAD_COMPARISON
WHERE UPPER(ENVIRONMENT) = 'PROD';
""",
        "dml": "",
    },
    {
        "name": "ELT_LOAD_COMPARISON_7D_VW",
        "description": "Recent 7-day load comparison by environment",
        "ddl": """
CREATE OR REPLACE VIEW {table_ref} AS
SELECT *
FROM {schema_prefix}ELT_LOAD_COMPARISON_ENV_VW
WHERE RUN_DATE >= CURRENT_DATE - INTERVAL '7 days';
""",
        "dml": "",
    },
    {
        "name": "ELT_JOB_RUN_ENV_VW",
        "description": "Environment-tagged job runs (DEV/TEST/UAT/PROD) for summaries",
        "ddl": """
CREATE OR REPLACE VIEW {table_ref} AS
SELECT 'DEV' AS ENVIRONMENT, RUN_ID, START_TIME, END_TIME, STATUS, ROWS_PROCESSED, ERROR_COUNT, EXECUTION_TIME, TRIGGER_SOURCE
FROM {schema_prefix}ELT_JOB_RUN
UNION ALL
SELECT 'TEST' AS ENVIRONMENT, RUN_ID, START_TIME, END_TIME, STATUS, ROWS_PROCESSED, ERROR_COUNT, EXECUTION_TIME, TRIGGER_SOURCE
FROM {schema_prefix}ELT_JOB_RUN
UNION ALL
SELECT 'UAT' AS ENVIRONMENT, RUN_ID, START_TIME, END_TIME, STATUS, ROWS_PROCESSED, ERROR_COUNT, EXECUTION_TIME, TRIGGER_SOURCE
FROM {schema_prefix}ELT_JOB_RUN
UNION ALL
SELECT 'PROD' AS ENVIRONMENT, RUN_ID, START_TIME, END_TIME, STATUS, ROWS_PROCESSED, ERROR_COUNT, EXECUTION_TIME, TRIGGER_SOURCE
FROM {schema_prefix}ELT_JOB_RUN;
""",
        "dml": "",
    },
    {
        "name": "ELT_ENVIRONMENT_SUMMARY_24H_VW",
        "description": "24h environment summary: active jobs, failures, max duration, max rows",
        "ddl": """
CREATE OR REPLACE VIEW {table_ref} AS
WITH unioned AS (
    SELECT * FROM {schema_prefix}ELT_JOB_RUN_ENV_VW
), recent AS (
    SELECT *
    FROM unioned
    WHERE START_TIME >= (CURRENT_TIMESTAMP - INTERVAL '24' HOUR)
)
SELECT
    ENVIRONMENT,
    COUNT(*) AS TOTAL_RUNS_24H,
    COUNT(*) FILTER (WHERE END_TIME IS NULL OR UPPER(COALESCE(STATUS, '')) IN ('RUNNING','IN_PROGRESS')) AS ACTIVE_JOBS,
    COUNT(*) FILTER (WHERE UPPER(COALESCE(STATUS, '')) IN ('FAILED','FAILURE','ERROR') OR ERROR_COUNT > 0) AS FAILURES_24H,
    MAX(DATEDIFF('second', START_TIME, END_TIME)) AS MAX_JOB_TIME_SEC_24H,
    MAX(ROWS_PROCESSED) AS MAX_ROWS_24H
FROM recent
GROUP BY ENVIRONMENT
ORDER BY ENVIRONMENT;
""",
        "dml": "",
    },
    {
        "name": "SCANDS_FINANCE_S2T",
        "description": "Source-to-target mapping for SCANDS Finance migration scope",
        "ddl": """
CREATE OR REPLACE TABLE {table_ref} (
    SOURCE_SCHEMA_NAME VARCHAR(16777216),
    SOURCE_TABLE_NAME VARCHAR(16777216),
    SOURCE_COLUMN_NAME VARCHAR(16777216),
    ORDINAL_POSITION DECIMAL(38, 0),
    SOURCE_DATA_TYPE VARCHAR(16777216),
    MAX_LENGTH DECIMAL(38, 0),
    PRECISION DECIMAL(38, 0),
    SCALE DECIMAL(38, 0),
    IS_NULLABLE DECIMAL(38, 0),
    TARGET_TABLE_NAME VARCHAR(16777216),
    TARGET_COLUMN_NAME VARCHAR(16777216),
    TARGET_DATA_TYPE VARCHAR(16777216),
    SOURCE_DATABASE VARCHAR(256) DEFAULT 'SCANDS_Finance',
    TARGET_DATABASE VARCHAR(256) DEFAULT 'DATA_VAULT'
);
""",
        "dml": """
INSERT INTO {table_ref} (
    SOURCE_SCHEMA_NAME, SOURCE_TABLE_NAME, SOURCE_COLUMN_NAME, ORDINAL_POSITION,
    SOURCE_DATA_TYPE, MAX_LENGTH, PRECISION, SCALE, IS_NULLABLE,
    TARGET_TABLE_NAME, TARGET_COLUMN_NAME, TARGET_DATA_TYPE,
    SOURCE_DATABASE, TARGET_DATABASE
) VALUES
    ('dbo', 'ACT_dimMemberMonths', 'MemberID', 1, 'varchar', 40, 0, 0, 0, 'ACT_DIM_MEMBER_MONTHS', 'MEMBER_ID', 'VARCHAR(40)', 'SCANDS_Finance', 'DATA_VAULT'),
    ('dbo', 'ACT_dimMemberMonths', 'YearMo', 2, 'varchar', 6, 0, 0, 0, 'ACT_DIM_MEMBER_MONTHS', 'YEAR_MO', 'VARCHAR(6)', 'SCANDS_Finance', 'DATA_VAULT'),
    ('dbo', 'ACT_dimMemberMonths', 'Contract_PBP', 3, 'varchar', 40, 0, 0, 1, 'ACT_DIM_MEMBER_MONTHS', 'CONTRACT_PBP', 'VARCHAR(40)', 'SCANDS_Finance', 'DATA_VAULT'),
    ('dbo', 'ACT_dimMemberMonths', 'ContractID', 4, 'varchar', 30, 0, 0, 0, 'ACT_DIM_MEMBER_MONTHS', 'CONTRACT_ID', 'VARCHAR(30)', 'SCANDS_Finance', 'DATA_VAULT'),
    ('dbo', 'ACT_dimMemberMonths', 'Gender', 5, 'varchar', 1, 0, 0, 1, 'ACT_DIM_MEMBER_MONTHS', 'GENDER', 'VARCHAR(1)', 'SCANDS_Finance', 'DATA_VAULT'),
    ('dbo', 'ACT_dimMemberMonths', 'ManagedPopulation', 7, 'varchar', 40, 0, 0, 1, 'ACT_DIM_MEMBER_MONTHS', 'MANAGED_POPULATION', 'VARCHAR(40)', 'SCANDS_Finance', 'DATA_VAULT'),
    ('dbo', 'ACT_dimMemberMonths', 'HeartCondition', 14, 'bit', 1, 1, 0, 1, 'ACT_DIM_MEMBER_MONTHS', 'HEART_CONDITION', 'BOOLEAN', 'SCANDS_Finance', 'DATA_VAULT'),
    ('dbo', 'ACT_outClaims_BD', 'MEMBERID', 1, 'varchar', 40, 0, 0, 1, 'ACT_OUT_CLAIMS_BD', 'MEMBERID', 'VARCHAR(40)', 'SCANDS_Finance', 'DATA_VAULT'),
    ('dbo', 'ACT_outClaims_BD', 'SERVICEDATE', 2, 'datetime', 8, 23, 3, 1, 'ACT_OUT_CLAIMS_BD', 'SERVICEDATE', 'TIMESTAMP_NTZ(3)', 'SCANDS_Finance', 'DATA_VAULT'),
    ('dbo', 'ACT_outClaims_BD', 'PAID_ADJ', 15, 'float', 8, 53, 0, 1, 'ACT_OUT_CLAIMS_BD', 'PAID_ADJ', 'FLOAT', 'SCANDS_Finance', 'DATA_VAULT'),
    ('dbo', 'ACT_outMemberMonths_BD', 'MemberID', 1, 'varchar', 40, 0, 0, 0, 'ACT_OUT_MEMBER_MONTHS_BD', 'MEMBER_ID', 'VARCHAR(40)', 'SCANDS_Finance', 'DATA_VAULT'),
    ('dbo', 'ACT_outMemberMonths_BD', 'PartC_risk_score_2014', 5, 'numeric', 5, 5, 2, 1, 'ACT_OUT_MEMBER_MONTHS_BD', 'PART_C_RISK_SCORE_2014', 'NUMBER(5,2)', 'SCANDS_Finance', 'DATA_VAULT');
""",
    },
    {
        "name": "VALIDATION_OBJECTS",
        "description": "Registry of validation targets used for compatibility and monitoring",
        "date_column": "CREATED_AT",
        "ddl": """
CREATE OR REPLACE TABLE {table_ref} (
    NAME VARCHAR(255) NOT NULL,
    FULLY_QUALIFIED_NAME VARCHAR(512) NOT NULL,
    DESCRIPTION VARCHAR(2000),
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);
""",
        "dml": """
INSERT INTO {table_ref} (NAME, FULLY_QUALIFIED_NAME, DESCRIPTION) VALUES
    ('Finance compat view', 'DATA_VAULT_TEMP.INFO_MART.ACT_DIM_MEMBER_MONTHS', 'Legacy compatibility projection for ACT member months'),
    ('Admin index base', 'DATA_VAULT_TEMP.INFO_MART.ADMIN_INDEX', 'Admin index view coverage for compatibility smoke test');
""",
    },
    {
        "name": "TEAM_MEMBERS",
        "description": "Roster of functional team members that Copilot references across pages",
        "date_column": "CREATED_AT",
        "ddl": """
CREATE OR REPLACE TABLE {table_ref} (
    TEAM VARCHAR(50) NOT NULL,
    MEMBER_NAME VARCHAR(200) NOT NULL,
    USERNAME VARCHAR(255) NOT NULL,
    RESPONSIBILITY VARCHAR(255),
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);
""",
        "dml": """
INSERT INTO {table_ref} (TEAM, MEMBER_NAME, USERNAME, RESPONSIBILITY) VALUES
    ('Ingestion Team', 'Ava Patel', 'ava.patel@example.com', 'Snowflake ingestion pipelines'),
    ('Modeling Team', 'Luis Romero', 'luis.romero@example.com', 'Semantic layer stewardship'),
    ('BI Team', 'Nora Chen', 'nora.chen@example.com', 'Executive dashboards and KPIs');
""",
    },
    {
        "name": "VALIDATION_METRICS_SOURCE",
        "description": "Source system ingestion validation metrics captured before Snowflake landing",
        "ddl": """
CREATE OR REPLACE TABLE {table_ref} (
    CHECK_TYPE VARCHAR(16777216),
    OBJECT_NAME VARCHAR(16777216),
    METRIC_NAME VARCHAR(16777216),
    METRIC_VALUE VARCHAR(16777216),
    SEVERITY VARCHAR(16777216),
    NOTES VARCHAR(16777216)
);
""",
        "dml": """
INSERT INTO {table_ref} (CHECK_TYPE, OBJECT_NAME, METRIC_NAME, METRIC_VALUE, SEVERITY, NOTES) VALUES
    ('TABLE', '"STAGE_DEV"."MEDHOK"."ADDITIONAL_PROVIDER_MEDICARE"', 'row_count', '70', 'INFO', NULL),
    ('TABLE', '"STAGE_DEV"."MEDHOK"."ADDITIONAL_PROVIDER_MEDICARE"', 'column_count_excluding_metadata', '32', 'INFO', NULL),
    ('INGESTION', '"STAGE_DEV"."MEDHOK"."ADDITIONAL_PROVIDER_MEDICARE"', 'distinct_file_prefix_count', '1', 'INFO', 'Distinct normalized prefixes from _FILE_NAME (UPPER; strips _YYYYMMDD_* suffix).'),
    ('INGESTION', '"STAGE_DEV"."MEDHOK"."ADDITIONAL_PROVIDER_MEDICARE"', 'file_prefixes_found', 'ADDITIONAL_PROVIDER_MEDICARE_', 'INFO', 'Comma-separated list of normalized file prefixes found in _FILE_NAME.'),
    ('KEY', '"STAGE_DEV"."MEDHOK"."ADDITIONAL_PROVIDER_MEDICARE"', 'business_key_columns_found', '1', 'INFO', 'Matched BUSINESS_KEY columns in INFORMATION_SCHEMA (case-insensitive; stripped optional quotes).'),
    ('KEY', '"STAGE_DEV"."MEDHOK"."ADDITIONAL_PROVIDER_MEDICARE"', 'business_key_null_key_row_count', '0', 'INFO', 'Rows where ANY business key column is NULL (within filter scope).'),
    ('KEY', '"STAGE_DEV"."MEDHOK"."ADDITIONAL_PROVIDER_MEDICARE"', 'business_key_null_key_row_pct', '0.0000', 'INFO', 'Default FAIL threshold: >= 1% null keys (within filter scope).'),
    ('KEY', '"STAGE_DEV"."MEDHOK"."ADDITIONAL_PROVIDER_MEDICARE"', 'business_key_distinct_count', '7', 'INFO', 'Distinct keys on rows where ALL key columns are NOT NULL (within filter scope).'),
    ('KEY', '"STAGE_DEV"."MEDHOK"."ADDITIONAL_PROVIDER_MEDICARE"', 'business_key_duplicate_count', '63', 'FAIL', 'Duplicate keys = non-null-key rows minus distinct keys (within filter scope).'),
    ('KEY', '"STAGE_DEV"."MEDHOK"."ADDITIONAL_PROVIDER_MEDICARE"', 'business_key_duplicate_pct', '90.0000', 'FAIL', 'Any duplicate business key is FAIL by default (within filter scope).'),
    ('KEY', '"STAGE_DEV"."MEDHOK"."ADDITIONAL_PROVIDER_MEDICARE"', 'business_key_min_value', '10', 'INFO', 'MIN over TO_VARCHAR("MHK_Additional_Provider_Internal_ID") (lexical for strings) within filter scope.'),
    ('KEY', '"STAGE_DEV"."MEDHOK"."ADDITIONAL_PROVIDER_MEDICARE"', 'business_key_max_value', '9', 'INFO', 'MAX over TO_VARCHAR("MHK_Additional_Provider_Internal_ID") (lexical for strings) within filter scope.'),
    ('INGESTION', '"STAGE_DEV"."MEDHOK"."ADDITIONAL_PROVIDER_MEDICARE"', 'created_at_column_used', 'Insert_Datetime', 'INFO', NULL),
    ('INGESTION', '"STAGE_DEV"."MEDHOK"."ADDITIONAL_PROVIDER_MEDICARE"', 'created_at_min', NULL, 'INFO', 'MIN TRY_TO_TIMESTAMP_LTZ(TO_VARCHAR("Insert_Datetime")) within filter scope.'),
    ('INGESTION', '"STAGE_DEV"."MEDHOK"."ADDITIONAL_PROVIDER_MEDICARE"', 'created_at_max', NULL, 'INFO', 'MAX TRY_TO_TIMESTAMP_LTZ(TO_VARCHAR("Insert_Datetime")) within filter scope.'),
    ('INGESTION', '"STAGE_DEV"."MEDHOK"."ADDITIONAL_PROVIDER_MEDICARE"', 'updated_at_column_used', 'Update_Datetime', 'INFO', NULL),
    ('INGESTION', '"STAGE_DEV"."MEDHOK"."ADDITIONAL_PROVIDER_MEDICARE"', 'updated_at_min', NULL, 'INFO', 'MIN TRY_TO_TIMESTAMP_LTZ(TO_VARCHAR("Update_Datetime")) within filter scope.'),
    ('INGESTION', '"STAGE_DEV"."MEDHOK"."ADDITIONAL_PROVIDER_MEDICARE"', 'updated_at_max', NULL, 'INFO', 'MAX TRY_TO_TIMESTAMP_LTZ(TO_VARCHAR("Update_Datetime")) within filter scope.'),
    ('TABLE', '"STAGE_DEV"."MEDHOK"."ADDITIONAL_PROVIDER_MEDICARE"', 'distinct_row_signature_count', '7', 'INFO', 'SHA2 over concatenated non-metadata columns (within filter scope).'),
    ('TABLE', '"STAGE_DEV"."MEDHOK"."ADDITIONAL_PROVIDER_MEDICARE"', 'duplicate_row_signature_count', '63', 'WARN', 'Duplicate rows detected by signature (may include legitimate repeats) within filter scope.'),
    ('TABLE', '"STAGE_DEV"."MEDHOK"."ADDITIONAL_PROVIDER_MEDICARE"', 'duplicate_row_signature_pct', '90.0000', 'FAIL', 'Default FAIL threshold: >= 0.5% signature dupes (within filter scope).');
""",
    },
    {
        "name": "VALIDATION_METRICS_TARGET",
        "description": "Snowflake ingestion validation metrics for comparison against source baselines",
        "ddl": """
CREATE OR REPLACE TABLE {table_ref} (
    CHECK_TYPE VARCHAR(16777216),
    OBJECT_NAME VARCHAR(16777216),
    METRIC_NAME VARCHAR(16777216),
    METRIC_VALUE VARCHAR(16777216),
    SEVERITY VARCHAR(16777216),
    NOTES VARCHAR(16777216)
);
""",
        "dml": """
INSERT INTO {table_ref} (CHECK_TYPE, OBJECT_NAME, METRIC_NAME, METRIC_VALUE, SEVERITY, NOTES) VALUES
    ('TABLE', '"STAGE_DEV"."MEDHOK"."ADDITIONAL_PROVIDER_MEDICARE"', 'row_count', '70', 'INFO', 'Snowflake staging row count.'),
    ('TABLE', '"STAGE_DEV"."MEDHOK"."ADDITIONAL_PROVIDER_MEDICARE"', 'column_count_excluding_metadata', '32', 'INFO', 'Metadata columns omitted for parity with SQL Server output.'),
    ('INGESTION', '"STAGE_DEV"."MEDHOK"."ADDITIONAL_PROVIDER_MEDICARE"', 'distinct_file_prefix_count', '1', 'INFO', 'All files landed with the same normalized prefix.'),
    ('INGESTION', '"STAGE_DEV"."MEDHOK"."ADDITIONAL_PROVIDER_MEDICARE"', 'file_prefixes_found', 'ADDITIONAL_PROVIDER_MEDICARE_', 'INFO', 'Derived from stage filenames after normalization.'),
    ('KEY', '"STAGE_DEV"."MEDHOK"."ADDITIONAL_PROVIDER_MEDICARE"', 'business_key_columns_found', '1', 'INFO', 'Business key alignment confirmed post-load.'),
    ('KEY', '"STAGE_DEV"."MEDHOK"."ADDITIONAL_PROVIDER_MEDICARE"', 'business_key_null_key_row_count', '0', 'INFO', 'Null business key count within Snowflake scope.'),
    ('KEY', '"STAGE_DEV"."MEDHOK"."ADDITIONAL_PROVIDER_MEDICARE"', 'business_key_null_key_row_pct', '0.0000', 'INFO', 'Null percentage threshold < 1% remains satisfied.'),
    ('KEY', '"STAGE_DEV"."MEDHOK"."ADDITIONAL_PROVIDER_MEDICARE"', 'business_key_distinct_count', '70', 'INFO', 'All rows present unique business keys after de-duplication.'),
    ('KEY', '"STAGE_DEV"."MEDHOK"."ADDITIONAL_PROVIDER_MEDICARE"', 'business_key_duplicate_count', '0', 'INFO', 'Duplicates removed during ingestion.'),
    ('KEY', '"STAGE_DEV"."MEDHOK"."ADDITIONAL_PROVIDER_MEDICARE"', 'business_key_duplicate_pct', '0.0000', 'INFO', 'Duplicate ratio remains below failure threshold.'),
    ('KEY', '"STAGE_DEV"."MEDHOK"."ADDITIONAL_PROVIDER_MEDICARE"', 'business_key_min_value', '10', 'INFO', 'Lexical MIN aligned with source expectation.'),
    ('KEY', '"STAGE_DEV"."MEDHOK"."ADDITIONAL_PROVIDER_MEDICARE"', 'business_key_max_value', '72', 'INFO', 'Lexical MAX based on Snowflake data scan.'),
    ('INGESTION', '"STAGE_DEV"."MEDHOK"."ADDITIONAL_PROVIDER_MEDICARE"', 'created_at_column_used', 'Insert_Datetime', 'INFO', NULL),
    ('INGESTION', '"STAGE_DEV"."MEDHOK"."ADDITIONAL_PROVIDER_MEDICARE"', 'created_at_min', NULL, 'INFO', 'Warehouse timestamps still pending backfill.'),
    ('INGESTION', '"STAGE_DEV"."MEDHOK"."ADDITIONAL_PROVIDER_MEDICARE"', 'created_at_max', NULL, 'INFO', 'Warehouse timestamps still pending backfill.'),
    ('INGESTION', '"STAGE_DEV"."MEDHOK"."ADDITIONAL_PROVIDER_MEDICARE"', 'updated_at_column_used', 'Update_Datetime', 'INFO', NULL),
    ('INGESTION', '"STAGE_DEV"."MEDHOK"."ADDITIONAL_PROVIDER_MEDICARE"', 'updated_at_min', NULL, 'INFO', 'Warehouse timestamps still pending backfill.'),
    ('INGESTION', '"STAGE_DEV"."MEDHOK"."ADDITIONAL_PROVIDER_MEDICARE"', 'updated_at_max', NULL, 'INFO', 'Warehouse timestamps still pending backfill.'),
    ('TABLE', '"STAGE_DEV"."MEDHOK"."ADDITIONAL_PROVIDER_MEDICARE"', 'distinct_row_signature_count', '70', 'INFO', 'All rows are unique after SHA2 signature check.'),
    ('TABLE', '"STAGE_DEV"."MEDHOK"."ADDITIONAL_PROVIDER_MEDICARE"', 'duplicate_row_signature_count', '0', 'INFO', 'No duplicate signatures detected.'),
    ('TABLE', '"STAGE_DEV"."MEDHOK"."ADDITIONAL_PROVIDER_MEDICARE"', 'duplicate_row_signature_pct', '0.0000', 'INFO', 'Signature duplication held below alert threshold.');
""",
    },
    {
        "name": "VALIDATION_MODEL_RUNS",
        "description": "Audit log for modelling validation executions (manual or scheduled).",
        "ddl": """
CREATE OR REPLACE TABLE {table_ref} (
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
""",
        "dml": """
INSERT INTO {table_ref} (
    RUN_ID, RUN_AT, RUN_TYPE, SCHEDULE_FLAG, RUN_ENVIRONMENT, RUN_STATUS, TABLE_NAME,
    SOURCE_DB, SOURCE_SCHEMA, TARGET_DB, TARGET_SCHEMA, SAMPLE_SIZE,
    JOIN_KEY, SOURCE_HIGH_WATERMARK, TARGET_HIGH_WATERMARK, EXCLUDE_COLS,
    GENERATED_JSON, GENERATED_BY, NOTES
) VALUES (
    '00000000-0000-0000-0000-000000000001', CURRENT_TIMESTAMP, 'manual', FALSE, 'DEV', 'success', 'DIM_SITE_ATTRIBUTES',
    'DATA_VAULT_TEMP', 'INFO_MART', '_DATA_VAULT_DEV_CHRIS', 'INFO_MART', 1000,
    'SITE_ATTR_ID', 'DW_LAST_UPDATED_DATE', 'SYSTEM_CREATE_DATE', 'DIM_SITE_ATTRIBUTES_KEY,SYSTEM_VERSION',
    TRUE, 'demo_user', 'Seed modelling validation run'
);
""",
    },
    {
        "name": "VALIDATION_MODEL_RESULTS",
        "description": "Step-level modelling validation outputs persisted as VARIANT payloads.",
        "ddl": """
CREATE OR REPLACE TABLE {table_ref} (
    RUN_ID VARCHAR(16777216) NOT NULL,
    STEP_NAME VARCHAR(255) NOT NULL,
    STATUS VARCHAR(50),
    ROW_COUNT NUMBER,
    RESULT_DATA VARIANT,
    EXECUTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (RUN_ID, STEP_NAME)
);
""",
        "dml": """
INSERT INTO {table_ref} (RUN_ID, STEP_NAME, STATUS, ROW_COUNT, RESULT_DATA)
SELECT
    '00000000-0000-0000-0000-000000000001' AS RUN_ID,
    'column_inventory' AS STEP_NAME,
    'success' AS STATUS,
    3 AS ROW_COUNT,
    {JSON_PAYLOAD} AS RESULT_DATA;
""",
    },
    {
        "name": "SCANDS_PROD_S2T",
        "description": "Source-to-target mapping for SCANDS Production workloads",
        "ddl": """
CREATE OR REPLACE TABLE {table_ref} (
    SOURCE_SCHEMA_NAME VARCHAR(16777216),
    SOURCE_TABLE_NAME VARCHAR(16777216),
    SOURCE_COLUMN_NAME VARCHAR(16777216),
    ORDINAL_POSITION DECIMAL(38, 0),
    SOURCE_DATA_TYPE VARCHAR(16777216),
    MAX_LENGTH DECIMAL(38, 0),
    PRECISION DECIMAL(38, 0),
    SCALE DECIMAL(38, 0),
    IS_NULLABLE DECIMAL(38, 0),
    TARGET_TABLE_NAME VARCHAR(16777216),
    TARGET_COLUMN_NAME VARCHAR(16777216),
    TARGET_DATA_TYPE VARCHAR(16777216),
    SOURCE_DATABASE VARCHAR(256) DEFAULT 'SCANDS_Prod',
    TARGET_DATABASE VARCHAR(256) DEFAULT 'DATA_VAULT'
);
""",
        "dml": """
INSERT INTO {table_ref} (
    SOURCE_SCHEMA_NAME, SOURCE_TABLE_NAME, SOURCE_COLUMN_NAME, ORDINAL_POSITION,
    SOURCE_DATA_TYPE, MAX_LENGTH, PRECISION, SCALE, IS_NULLABLE,
    TARGET_TABLE_NAME, TARGET_COLUMN_NAME, TARGET_DATA_TYPE,
    SOURCE_DATABASE, TARGET_DATABASE
) VALUES
    ('dbo', 'Admin_Index', 'TableName', 1, 'varchar', 50, 0, 0, 1, 'ADMIN_INDEX', 'TABLE_NAME', 'VARCHAR(50)', 'SCANDS_Prod', 'DATA_VAULT'),
    ('dbo', 'Admin_Index', 'ColumnName', 2, 'varchar', -1, 0, 0, 1, 'ADMIN_INDEX', 'COLUMN_NAME', 'VARCHAR(16777216)', 'SCANDS_Prod', 'DATA_VAULT'),
    ('dbo', 'Admin_Index', 'IndexName', 4, 'varchar', 100, 0, 0, 1, 'ADMIN_INDEX', 'INDEX_NAME', 'VARCHAR(100)', 'SCANDS_Prod', 'DATA_VAULT'),
    ('dbo', 'Admin_Table', 'TableName', 1, 'varchar', 50, 0, 0, 1, 'ADMIN_TABLE', 'TABLE_NAME', 'VARCHAR(50)', 'SCANDS_Prod', 'DATA_VAULT'),
    ('dbo', 'Admin_Table', 'DWLastUpdatedDate', 2, 'datetime', 8, 23, 3, 1, 'ADMIN_TABLE', 'DW_LAST_UPDATED_DATE', 'TIMESTAMP_NTZ(3)', 'SCANDS_Prod', 'DATA_VAULT'),
    ('dbo', 'Admin_Table', 'TableID', 3, 'smallint', 2, 5, 0, 0, 'ADMIN_TABLE', 'TABLE_ID', 'SMALLINT', 'SCANDS_Prod', 'DATA_VAULT'),
    ('dbo', 'B_AuthDiagnosis', 'AuthId', 1, 'varchar', 50, 0, 0, 1, 'BRIDGE_AUTH_DIAGNOSIS', 'AUTH_ID', 'VARCHAR(50)', 'SCANDS_Prod', 'DATA_VAULT'),
    ('dbo', 'B_AuthDiagnosis', 'DiagnosisDesc', 10, 'varchar', 500, 0, 0, 1, 'BRIDGE_AUTH_DIAGNOSIS', 'DIAGNOSIS_DESC', 'VARCHAR(500)', 'SCANDS_Prod', 'DATA_VAULT'),
    ('dbo', 'B_AuthDiagnosis', 'CreatedDate', 19, 'datetime', 8, 23, 3, 1, 'BRIDGE_AUTH_DIAGNOSIS', 'CREATED_DATE', 'TIMESTAMP_NTZ(3)', 'SCANDS_Prod', 'DATA_VAULT'),
    ('dbo', 'B_AuthDiagnosis_BU_Final', 'DiagnosisCd', 9, 'varchar', 10, 0, 0, 1, 'BRIDGE_AUTH_DIAGNOSIS_BU_FINAL', 'DIAGNOSIS_CD', 'VARCHAR(10)', 'SCANDS_Prod', 'DATA_VAULT'),
    ('dbo', 'B_ClaimAdjustmentSegment', 'ClaimID', 1, 'char', 12, 0, 0, 0, 'BRIDGE_CLAIM_ADJUSTMENT_SEGMENT', 'CLAIM_ID', 'CHAR(12)', 'SCANDS_Prod', 'DATA_VAULT'),
    ('dbo', 'B_ClaimAdjustmentSegment', 'Amount', 6, 'money', 8, 19, 4, 1, 'BRIDGE_CLAIM_ADJUSTMENT_SEGMENT', 'AMOUNT', 'NUMBER(19,4)', 'SCANDS_Prod', 'DATA_VAULT');
""",
    },
    {
        "name": "SCANDS_QUALITYRISK_S2T",
        "description": "Source-to-target mapping for Quality & Risk workloads",
        "ddl": """
CREATE OR REPLACE TABLE {table_ref} (
    SOURCE_SCHEMA_NAME VARCHAR(16777216),
    SOURCE_TABLE_NAME VARCHAR(16777216),
    SOURCE_COLUMN_NAME VARCHAR(16777216),
    ORDINAL_POSITION DECIMAL(38, 0),
    SOURCE_DATA_TYPE VARCHAR(16777216),
    MAX_LENGTH DECIMAL(38, 0),
    PRECISION DECIMAL(38, 0),
    SCALE DECIMAL(38, 0),
    IS_NULLABLE DECIMAL(38, 0),
    TARGET_TABLE_NAME VARCHAR(16777216),
    TARGET_COLUMN_NAME VARCHAR(16777216),
    TARGET_DATA_TYPE VARCHAR(16777216),
    SOURCE_DATABASE VARCHAR(256) DEFAULT 'SCANDS_QualityRisk',
    TARGET_DATABASE VARCHAR(256) DEFAULT 'DATA_VAULT'
);
""",
        "dml": """
INSERT INTO {table_ref} (
    SOURCE_SCHEMA_NAME, SOURCE_TABLE_NAME, SOURCE_COLUMN_NAME, ORDINAL_POSITION,
    SOURCE_DATA_TYPE, MAX_LENGTH, PRECISION, SCALE, IS_NULLABLE,
    TARGET_TABLE_NAME, TARGET_COLUMN_NAME, TARGET_DATA_TYPE,
    SOURCE_DATABASE, TARGET_DATABASE
) VALUES
    ('dbo', 'Admin_SnapDate', 'SnapDate', 1, 'datetime', 8, 23, 3, 1, 'ADMIN_SNAP_DATE', 'SNAP_DATE', 'TIMESTAMP_NTZ(3)', 'SCANDS_QualityRisk', 'DATA_VAULT'),
    ('dbo', 'B_QR_DeniedReferClaim', 'Mbr_Id', 1, 'varchar', 20, 0, 0, 1, 'BRIDGE_QR_DENIED_REFER_CLAIM', 'MBR_ID', 'VARCHAR(20)', 'SCANDS_QualityRisk', 'DATA_VAULT'),
    ('dbo', 'B_QR_DeniedReferClaim', 'DeniedReferClaimCount', 7, 'int', 4, 10, 0, 1, 'BRIDGE_QR_DENIED_REFER_CLAIM', 'DENIED_REFER_CLAIM_COUNT', 'INT', 'SCANDS_QualityRisk', 'DATA_VAULT'),
    ('dbo', 'B_QR_DeniedReferClaim', 'MostRecentFlag', 8, 'char', 1, 0, 0, 1, 'BRIDGE_QR_DENIED_REFER_CLAIM', 'MOST_RECENT_FLAG', 'CHAR(1)', 'SCANDS_QualityRisk', 'DATA_VAULT'),
    ('dbo', 'B_QR_DeniedReferClaim', 'UpdatedDateTime', 14, 'datetime', 8, 23, 3, 1, 'BRIDGE_QR_DENIED_REFER_CLAIM', 'UPDATED_DATE_TIME', 'TIMESTAMP_NTZ(3)', 'SCANDS_QualityRisk', 'DATA_VAULT'),
    ('dbo', 'B_QR_DeniedReferClaim', 'DeniedReferClaimSeqID', 26, 'varchar', 30, 0, 0, 1, 'BRIDGE_QR_DENIED_REFER_CLAIM', 'DENIED_REFER_CLAIM_SEQ_ID', 'VARCHAR(30)', 'SCANDS_QualityRisk', 'DATA_VAULT'),
    ('dbo', 'B_QR_HCBSCkList', 'Mbr_Id', 1, 'varchar', 20, 0, 0, 1, 'BRIDGE_QR_HCBS_CK_LIST', 'MBR_ID', 'VARCHAR(20)', 'SCANDS_QualityRisk', 'DATA_VAULT'),
    ('dbo', 'B_QR_HCBSCkList', 'EventCatName', 13, 'varchar', 1023, 0, 0, 1, 'BRIDGE_QR_HCBS_CK_LIST', 'EVENT_CAT_NAME', 'VARCHAR(1023)', 'SCANDS_QualityRisk', 'DATA_VAULT'),
    ('dbo', 'B_QR_HCBSCkList', 'UpdatedDateTime', 18, 'datetime', 8, 23, 3, 1, 'BRIDGE_QR_HCBS_CK_LIST', 'UPDATED_DATE_TIME', 'TIMESTAMP_NTZ(3)', 'SCANDS_QualityRisk', 'DATA_VAULT'),
    ('dbo', 'B_QR_InPersonInstitute', 'Mbr_Id', 1, 'varchar', 20, 0, 0, 1, 'BRIDGE_QR_IN_PERSON_INSTITUTE', 'MBR_ID', 'VARCHAR(20)', 'SCANDS_QualityRisk', 'DATA_VAULT'),
    ('dbo', 'B_QR_InPersonInstitute', 'EventCatName', 13, 'varchar', 1023, 0, 0, 1, 'BRIDGE_QR_IN_PERSON_INSTITUTE', 'EVENT_CAT_NAME', 'VARCHAR(1023)', 'SCANDS_QualityRisk', 'DATA_VAULT'),
    ('dbo', 'B_QR_InPersonInstitute', 'UpdatedDateTime', 18, 'datetime', 8, 23, 3, 1, 'BRIDGE_QR_IN_PERSON_INSTITUTE', 'UPDATED_DATE_TIME', 'TIMESTAMP_NTZ(3)', 'SCANDS_QualityRisk', 'DATA_VAULT');
"""
    },
    {
        "name": "MAPPING_RULES",
        "description": "Naming and transformation rules for source-to-target mappings",
        "date_column": "LAST_MODIFIED_DATE",
        "ddl": """
CREATE OR REPLACE TABLE {table_ref} (
    RULE_ID VARCHAR(16777216),
    RULE_NAME VARCHAR(256),
    RULE_TYPE VARCHAR(50),
    SOURCE_PATTERN VARCHAR(256),
    TARGET_PATTERN VARCHAR(256),
    DESCRIPTION VARCHAR(16777216),
    EXAMPLE_SOURCE VARCHAR(256),
    EXAMPLE_TARGET VARCHAR(256),
    IS_ACTIVE BOOLEAN DEFAULT TRUE,
    APPLIES_TO VARCHAR(50),
    SORT_ORDER INTEGER DEFAULT 0,
    CREATED_DATE TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    LAST_MODIFIED_DATE TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CREATED_BY VARCHAR(255),
    LAST_MODIFIED_BY VARCHAR(255)
);
""",
        "dml": """
INSERT INTO {table_ref} (
    RULE_ID, RULE_NAME, RULE_TYPE, SOURCE_PATTERN, TARGET_PATTERN, DESCRIPTION,
    EXAMPLE_SOURCE, EXAMPLE_TARGET, IS_ACTIVE, APPLIES_TO, SORT_ORDER,
    CREATED_DATE, LAST_MODIFIED_DATE, CREATED_BY
) VALUES
    ('RULE_20250127_001', 'Fact Table Prefix', 'PREFIX_MAPPING', 'F_', 'Fact_', 'Convert F_ prefix to Fact_ prefix for fact tables', 'F_SalesTransactions', 'Fact_SalesTransactions', TRUE, 'TABLE', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'system'),
    ('RULE_20250127_002', 'Dimension Table Prefix', 'PREFIX_MAPPING', 'D_', 'Dim_', 'Convert D_ prefix to Dim_ prefix for dimension tables', 'D_Customer', 'Dim_Customer', TRUE, 'TABLE', 2, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'system'),
    ('RULE_20250127_003', 'Table Case Conversion', 'CASE_CONVERSION', 'camelCase', 'UPPER_SNAKE_CASE', 'Convert camelCase table names to UPPER_SNAKE_CASE', 'CustomerAddress', 'CUSTOMER_ADDRESS', TRUE, 'TABLE', 3, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'system'),
    ('RULE_20250127_004', 'Column Case Conversion', 'CASE_CONVERSION', 'camelCase', 'UPPER_SNAKE_CASE', 'Convert camelCase column names to UPPER_SNAKE_CASE', 'customerID', 'CUSTOMER_ID', TRUE, 'COLUMN', 4, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'system'),
    ('RULE_20250127_005', 'Bridge Table Naming', 'PREFIX_MAPPING', 'BRIDGE_', 'Bridge_', 'Convert BRIDGE_ prefix to Bridge_ for bridge/junction tables', 'BRIDGE_OrderProduct', 'Bridge_OrderProduct', TRUE, 'TABLE', 5, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'system');
"""
    },
    {
        "name": "MAPPING_REVIEW_CORRECTIONS",
        "description": "Tracks mapping corrections and review status for source-to-target mappings",
        "date_column": "LAST_MODIFIED_DATE",
        "ddl": """
CREATE OR REPLACE TABLE {table_ref} (
    MAPPING_ID VARCHAR(16777216),
    OBJECT_TYPE VARCHAR(50),
    SOURCE_TABLE_NAME VARCHAR(16777216),
    SOURCE_COLUMN_NAME VARCHAR(16777216),
    ORIGINAL_TARGET_NAME VARCHAR(16777216),
    SUGGESTED_TARGET_NAME VARCHAR(16777216),
    UPDATED_TARGET_NAME VARCHAR(16777216),
    NEEDS_REVIEW BOOLEAN DEFAULT FALSE,
    STATUS VARCHAR(50) DEFAULT 'PENDING',
    CREATED_DATE TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    LAST_MODIFIED_DATE TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CREATED_BY VARCHAR(255),
    LAST_MODIFIED_BY VARCHAR(255),
    NOTES VARCHAR(16777216)
);
""",
        "dml": """
INSERT INTO {table_ref} (
    MAPPING_ID, OBJECT_TYPE, SOURCE_TABLE_NAME, SOURCE_COLUMN_NAME,
    ORIGINAL_TARGET_NAME, SUGGESTED_TARGET_NAME, UPDATED_TARGET_NAME,
    NEEDS_REVIEW, STATUS, CREATED_DATE, LAST_MODIFIED_DATE, CREATED_BY, NOTES
) VALUES
    ('MAP_20250127_001', 'TABLE', 'B_AuthDiagnosis_BU_Final', NULL, 'BRIDGE_AUTH_DIAGNOSIS_BU_FINAL', 'BRIDGE_AUTH_DIAGNOSIS_BU_FINAL', 'BRIDGE_AUTH_DIAGNOSIS_FINAL', TRUE, 'NEEDS_REVIEW', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'data_team', 'Review requested - naming convention mismatch'),
    ('MAP_20250127_002', 'COLUMN', 'B_ClaimAdjustmentSegment', 'ClaimID', 'CLAIM_ID', 'CLAIM_ID', 'CLAIM_ID_PK', FALSE, 'MODIFIED', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'data_team', 'Updated to include _PK suffix for primary keys'),
    ('MAP_20250127_003', 'TABLE', 'B_QR_DeniedReferClaim', NULL, 'BRIDGE_QR_DENIED_REFER_CLAIM', 'BRIDGE_QR_DENIED_REFER_CLAIM', NULL, TRUE, 'NEEDS_REVIEW', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'quality_team', 'Pending approval - complex mapping'),
    ('MAP_20250127_004', 'COLUMN', 'Admin_SnapDate', 'SnapDate', 'SNAP_DATE', 'SNAP_DATE', 'SNAP_DATE_TIME', FALSE, 'MODIFIED', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'data_team', 'Changed to SNAP_DATE_TIME for clarity'),
    ('MAP_20250127_005', 'TABLE', 'B_QR_HCBSCkList', NULL, 'BRIDGE_QR_HCBS_CK_LIST', 'BRIDGE_QR_HCBS_CHECKLIST', NULL, TRUE, 'NEEDS_REVIEW', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'quality_team', 'Discussion needed on abbreviation expansion'),
    ('MAP_20250127_006', 'COLUMN', 'B_QR_HCBSCkList', 'EventCatName', 'EVENT_CAT_NAME', 'EVENT_CATEGORY_NAME', 'EVENT_CATEGORY_NAME', FALSE, 'MODIFIED', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'quality_team', 'Expanded abbreviation for consistency');
"""
    },
]


def get_table_names() -> List[str]:
    """Return list of all test table names."""
    return [t["name"] for t in TEST_TABLES]


# Tables that should only be created in local (DuckDB) test runs.
# When running against Snowflake (deployed), these should be referenced
# from the production location (DATA_VAULT_DEV.INFO_MART) instead of
# being created by the test setup.
LOCAL_ONLY_TABLES = {
    "SCANDS_FINANCE_S2T",
    "SCANDS_PROD_S2T",
    "SCANDS_QUALITYRISK_S2T",
}

# Mock/Test data tables - sample data for demos and local testing
MOCK_DATA_TABLES = {
    "TESTTABLE",
    "SAMPLE_DATA",
    "DAILY_METRICS",
}

# Metadata/App storage tables - where the app persists its own configuration, audit logs, etc.
METADATA_TABLES = {
    "METADATA_CONFIG_TABLE_ELT",
    "METADATA_CONFIG_TABLE_ELT_ADHOC",
    "METADATA_CONFIG_TABLE_ELT_QUERY",
    "VALIDATION_MODEL_RESULTS",
    "VALIDATION_MODEL_RUNS",
    "VALIDATION_METRICS_TARGET",
    "VALIDATION_METRICS_SOURCE",
    "TEAM_MEMBERS",
    "VALIDATION_OBJECTS",
    "ELT_LOAD_COMPARISON",
    "METADATA_SQL_SERVER_LOOKUP",
    "MAPPING_RULES",
    "MAPPING_REVIEW_CORRECTIONS",
    "SCANDS_FINANCE_S2T",
    "SCANDS_PROD_S2T",
    "SCANDS_QUALITYRISK_S2T",
}


def get_table_date_column(table_name: str) -> Optional[str]:
    """Get the date column for a table (used for max date status check)."""
    for t in TEST_TABLES:
        if t["name"].upper() == table_name.upper():
            return t.get("date_column")
    return None


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
        schema_prefix: Optional schema prefix (uses default DATA_VAULT_TEMP.MIGRATION for test tables)
    
    Returns:
        Formatted DDL statement
    """
    defn = get_table_definition(table_name)
    # All test tables are created in DATA_VAULT_TEMP.MIGRATION (temp workspace for development/testing)
    if schema_prefix:
        table_ref = f"{schema_prefix}.{table_name}"
    else:
        table_ref = table_name
    # For view DDLs we also need raw schema_prefix for referencing other objects
    schema_prefix_with_dot = f"{schema_prefix}." if schema_prefix else ""
    template = defn["ddl"]
    return template.replace("{table_ref}", table_ref).replace("{schema_prefix}", schema_prefix_with_dot).strip()


def format_dml(table_name: str, schema_prefix: str = "") -> str:
    """
    Format DML for a table with optional schema prefix.
    
    Args:
        table_name: Name of the table
        schema_prefix: Optional schema prefix (uses default DATA_VAULT_TEMP.MIGRATION for test tables)
    
    Returns:
        Formatted DML statement
    """
    defn = get_table_definition(table_name)
    # Views don't have DML - return empty string
    if "dml" not in defn:
        return ""
    # All test tables are created in DATA_VAULT_TEMP.MIGRATION (temp workspace for development/testing)
    if schema_prefix:
        table_ref = f"{schema_prefix}.{table_name}"
    else:
        table_ref = table_name
    template = defn["dml"]

    # Inject JSON payloads for variant columns without leaving brace placeholders in final SQL
    if table_name.upper() == "VALIDATION_MODEL_RESULTS":
        payload = json_literal('[{"COLUMN_NAME":"COL_A","IN_SOURCE":"YES","IN_TARGET":"YES"}]')
        template = template.replace("{JSON_PAYLOAD}", payload)

    return template.replace("{table_ref}", table_ref).strip()


def get_all_ddl(schema_prefix: str = "") -> List[str]:
    """Get all DDL statements for all test tables."""
    return [format_ddl(t["name"], schema_prefix) for t in TEST_TABLES]


def get_all_dml(schema_prefix: str = "") -> List[str]:
    """Get all DML statements for all test tables."""
    return [format_dml(t["name"], schema_prefix) for t in TEST_TABLES]


def get_mock_data_tables() -> List[Dict[str, Any]]:
    """Get mock/test data table definitions."""
    return [t for t in TEST_TABLES if t["name"].upper() in MOCK_DATA_TABLES]


def get_metadata_tables() -> List[Dict[str, Any]]:
    """Get metadata/app storage table definitions."""
    return [t for t in TEST_TABLES if t["name"].upper() in METADATA_TABLES]


def get_setup_sql_statements(
    table_list: List[Dict[str, Any]],
    schema_prefix: str = "",
    include_dml: bool = True,
) -> Dict[str, str]:
    """
    Generate DDL and DML statements for a list of tables.
    
    Args:
        table_list: List of table definitions
        schema_prefix: Schema prefix for Snowflake (e.g., "DATA_VAULT_DEV.INFO_MART")
        include_dml: Whether to include DML (INSERT statements)
    
    Returns:
        Dictionary mapping table name to combined DDL + DML as a single string
    """
    statements = {}
    for table_def in table_list:
        table_name = table_def["name"]
        ddl = format_ddl(table_name, schema_prefix)
        dml = format_dml(table_name, schema_prefix) if include_dml else ""
        # Combine DDL and DML
        combined = f"{ddl}\n\n{dml}" if dml else ddl
        statements[table_name] = combined
    return statements


def get_all_setup_sql(schema_prefix: str = "") -> str:
    """Get all DDL+DML statements concatenated for "Setup All Data"."""
    statements = []
    for table_def in TEST_TABLES:
        table_name = table_def["name"]
        ddl = format_ddl(table_name, schema_prefix)
        dml = format_dml(table_name, schema_prefix)
        if dml:
            statements.append(f"{ddl}\n\n{dml}")
        else:
            statements.append(ddl)
    return "\n\n".join(statements)


def get_mock_data_setup_sql(schema_prefix: str = "") -> str:
    """Get DDL+DML statements for mock data tables only."""
    statements = []
    for table_def in get_mock_data_tables():
        table_name = table_def["name"]
        ddl = format_ddl(table_name, schema_prefix)
        dml = format_dml(table_name, schema_prefix)
        if dml:
            statements.append(f"{ddl}\n\n{dml}")
        else:
            statements.append(ddl)
    return "\n\n".join(statements)


def get_metadata_setup_sql(schema_prefix: str = "") -> str:
    """Get DDL+DML statements for metadata tables only."""
    statements = []
    for table_def in get_metadata_tables():
        table_name = table_def["name"]
        ddl = format_ddl(table_name, schema_prefix)
        dml = format_dml(table_name, schema_prefix)
        if dml:
            statements.append(f"{ddl}\n\n{dml}")
        else:
            statements.append(ddl)
    return "\n\n".join(statements)
