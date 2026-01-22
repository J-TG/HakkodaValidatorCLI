"""
Test data definitions - single source of truth for DDL and DML across all runtime modes.

This module defines all test tables and sample data used for development and testing.
The same definitions are used for:
- DuckDB (local in-memory)
- Snowflake (local connector)
- Snowflake (deployed via Snowpark)
"""

from typing import List, Dict, Any, Optional

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
]


def get_table_names() -> List[str]:
    """Return list of all test table names."""
    return [t["name"] for t in TEST_TABLES]


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
        schema_prefix: Optional schema prefix (e.g., "DATA_VAULT_DEV.INFO_MART")
    
    Returns:
        Formatted DDL statement
    """
    defn = get_table_definition(table_name)
    if schema_prefix:
        table_ref = f"{schema_prefix}.{table_name}"
    else:
        table_ref = table_name
    template = defn["ddl"]
    return template.replace("{table_ref}", table_ref).strip()


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
    template = defn["dml"]
    return template.replace("{table_ref}", table_ref).strip()


def get_all_ddl(schema_prefix: str = "") -> List[str]:
    """Get all DDL statements for all test tables."""
    return [format_ddl(t["name"], schema_prefix) for t in TEST_TABLES]


def get_all_dml(schema_prefix: str = "") -> List[str]:
    """Get all DML statements for all test tables."""
    return [format_dml(t["name"], schema_prefix) for t in TEST_TABLES]
