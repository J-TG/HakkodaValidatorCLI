/*
================================================================================
INFO MART DATA COMPARISON SCRIPT
================================================================================
Compares data between:
  - SOURCE: DATA_VAULT_TEMP.INFO_MART (legacy EDW one-time loads)
  - TARGET: Developer/DEV Info Mart tables

All queries execute directly - no copy/paste needed.
Uses OBJECT_CONSTRUCT and FLATTEN for dynamic column comparison.
================================================================================
*/

-- ============================================================================
-- SCHEDULING ARTIFACTS (replace <FP_TABLE_NAME> and <ENV> before running)
-- ============================================================================

CREATE OR REPLACE TABLE DATA_VAULT_TEMP.MIGRATION_COPILOT.VALIDATION_<FP_TABLE_NAME>_<ENV>_COMMAND (
        COMMAND_ID NUMBER(38,0) NOT NULL AUTOINCREMENT START 1 INCREMENT 1 NOORDER,
        SQL_COMMAND VARCHAR(16777216),
        PRIMARY KEY (COMMAND_ID)
);

CREATE OR REPLACE TASK DATA_VAULT_TEMP.MIGRATION_COPILOT.VALIDATION_<FP_TABLE_NAME>_<ENV>_TASK_RUNBATCHSQL
        WAREHOUSE = STAGE_DEV_WH
        SCHEDULE = 'USING CRON 0 8 * * * UTC'
AS
        CALL DATA_VAULT_TEMP.MIGRATION_COPILOT.RUNBATCHSQL(
                '
                        SELECT sql_command
                        FROM DATA_VAULT_TEMP.MIGRATION_COPILOT.VALIDATION_<FP_TABLE_NAME>_<ENV>_COMMAND
                        ORDER BY command_id
                '
        );

CREATE OR REPLACE PROCEDURE DATA_VAULT_TEMP.MIGRATION_COPILOT.RUNBATCHSQL("SQL_STATEMENT" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
        DECLARE
            sql_command VARCHAR;
            res RESULTSET;
        BEGIN
            res := (EXECUTE IMMEDIATE :SQL_STATEMENT);

            LET c1 CURSOR FOR res;

            FOR row_variable IN c1 DO
                sql_command := row_variable.sql_command;
                EXECUTE IMMEDIATE :sql_command;
            END FOR;
            RETURN ''Sql executed'';
        END;
';

-- ============================================================================
-- CONFIGURATION
-- ============================================================================

-- Table/View names (usually identical)
SET TABLE_NAME = 'DIM_SITE_ATTRIBUTES';

-- Source: Legacy EDW data loaded into DATA_VAULT_TEMP
SET SOURCE_DATABASE = 'DATA_VAULT_TEMP';
SET SOURCE_SCHEMA = 'INFO_MART';

-- Target: New Info Mart (developer DB or shared DEV)
SET TARGET_DATABASE = '_DATA_VAULT_DEV_CHRIS';
SET TARGET_SCHEMA = 'INFO_MART';

-- Sample size (number of rows to compare)
SET SAMPLE_SIZE = 1000;

-- Join key: Business key column that uniquely identifies a row in BOTH tables
-- NOTE: Must be a single column name (case-sensitive as stored in Snowflake)
SET JOIN_KEY = 'SITE_ATTR_ID';

-- High Watermark: Timestamp column that represents the last time data was loaded
SET SOURCE_HIGH_WATERMARK = 'DW_LAST_UPDATED_DATE';
SET TARGET_HIGH_WATERMARK = 'SYSTEM_CREATE_DATE';

-- Columns to EXCLUDE from comparison (comma-separated, case-insensitive)
-- These exist in one or both tables but should not be compared
SET EXCLUDE_COLS = 'DIM_SITE_ATTRIBUTES_KEY,SYSTEM_VERSION,SYSTEM_CURRENT_FLAG,SYSTEM_START_DATE,SYSTEM_END_DATE,SYSTEM_CREATE_DATE,SYSTEM_UPDATE_DATE,DW_LAST_UPDATED_DATE';

-- Derived fully-qualified names
SET SOURCE_FQ = $SOURCE_DATABASE || '.' || $SOURCE_SCHEMA || '.' || $TABLE_NAME;
SET TARGET_FQ = $TARGET_DATABASE || '.' || $TARGET_SCHEMA || '.' || $TABLE_NAME;
SET SOURCE_INFO_SCHEMA_FQ = $SOURCE_DATABASE || '.INFORMATION_SCHEMA.COLUMNS';
SET TARGET_INFO_SCHEMA_FQ = $TARGET_DATABASE || '.INFORMATION_SCHEMA.COLUMNS';


-- ============================================================================
-- SECTION 1: Column Inventory - What columns exist in each table?
-- ============================================================================

WITH source_cols AS (
    SELECT COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION
    FROM IDENTIFIER($SOURCE_INFO_SCHEMA_FQ)
    WHERE TABLE_SCHEMA = $SOURCE_SCHEMA AND TABLE_NAME = $TABLE_NAME
),
target_cols AS (
    SELECT COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION
    FROM IDENTIFIER($TARGET_INFO_SCHEMA_FQ)
    WHERE TABLE_SCHEMA = $TARGET_SCHEMA AND TABLE_NAME = $TABLE_NAME
),
excluded AS (
    SELECT UPPER(TRIM(VALUE)) AS COL 
    FROM TABLE(SPLIT_TO_TABLE($EXCLUDE_COLS, ',')) 
    WHERE TRIM(VALUE) != ''
)
SELECT 
    COALESCE(s.COLUMN_NAME, t.COLUMN_NAME) AS COLUMN_NAME,
    CASE WHEN s.COLUMN_NAME IS NOT NULL THEN 'YES' ELSE 'NO' END AS IN_SOURCE,
    CASE WHEN t.COLUMN_NAME IS NOT NULL THEN 'YES' ELSE 'NO' END AS IN_TARGET,
    s.DATA_TYPE AS SOURCE_TYPE,
    t.DATA_TYPE AS TARGET_TYPE,
    CASE 
        WHEN UPPER(COALESCE(s.COLUMN_NAME, t.COLUMN_NAME)) = UPPER($JOIN_KEY) THEN 'JOIN KEY'
        WHEN ex.COL IS NOT NULL THEN 'EXCLUDED'
        WHEN s.COLUMN_NAME IS NULL THEN 'TARGET ONLY (skip)'
        WHEN t.COLUMN_NAME IS NULL THEN 'SOURCE ONLY (skip)'
        ELSE 'COMPARE'
    END AS ACTION
FROM source_cols s
FULL OUTER JOIN target_cols t ON UPPER(s.COLUMN_NAME) = UPPER(t.COLUMN_NAME)
LEFT JOIN excluded ex ON UPPER(COALESCE(s.COLUMN_NAME, t.COLUMN_NAME)) = ex.COL
ORDER BY 
    CASE 
        WHEN UPPER(COALESCE(s.COLUMN_NAME, t.COLUMN_NAME)) = UPPER($JOIN_KEY) THEN 1
        WHEN s.COLUMN_NAME IS NOT NULL AND t.COLUMN_NAME IS NOT NULL AND ex.COL IS NULL THEN 2
        ELSE 3
    END,
    COALESCE(s.ORDINAL_POSITION, t.ORDINAL_POSITION, 999);


-- ============================================================================
-- SECTION 2: Row Count & High Watermark Comparison 
-- ============================================================================

SELECT 
    'SOURCE' AS DATASET, 
    $SOURCE_FQ AS TABLE_NAME,
    COUNT(*) AS ROW_COUNT,
    MAX(IDENTIFIER($SOURCE_HIGH_WATERMARK)) AS MAX_LOAD_DATE
FROM IDENTIFIER($SOURCE_FQ)
UNION ALL
SELECT 
    'TARGET' AS DATASET, 
    $TARGET_FQ AS TABLE_NAME,
    COUNT(*) AS ROW_COUNT, 
    MAX(IDENTIFIER($TARGET_HIGH_WATERMARK)) AS MAX_LOAD_DATE
FROM IDENTIFIER($TARGET_FQ);


-- ============================================================================
-- SECTION 3: Row-Level Match Summary
-- ============================================================================
/*
Shows how many rows: MATCH, have MISMATCHES, or are MISSING
*/

WITH excluded AS (
    SELECT UPPER(TRIM(VALUE)) AS COL 
    FROM TABLE(SPLIT_TO_TABLE($EXCLUDE_COLS, ',')) 
    WHERE TRIM(VALUE) != ''
),
source_data AS (
    SELECT 
        GET(OBJECT_CONSTRUCT(*), $JOIN_KEY)::VARCHAR AS join_key,
        OBJECT_CONSTRUCT(*) AS all_cols
    FROM IDENTIFIER($SOURCE_FQ)
    SAMPLE ($SAMPLE_SIZE ROWS)
),
target_data AS (
    SELECT 
        GET(OBJECT_CONSTRUCT(*), $JOIN_KEY)::VARCHAR AS join_key,
        OBJECT_CONSTRUCT(*) AS all_cols
    FROM IDENTIFIER($TARGET_FQ)
),
-- Flatten source to get column/value pairs
source_flat AS (
    SELECT 
        s.join_key,
        f.key AS col_name,
        f.value::VARCHAR AS col_value
    FROM source_data s,
    LATERAL FLATTEN(input => s.all_cols) f
    WHERE UPPER(f.key) != UPPER($JOIN_KEY)
      AND NOT EXISTS (SELECT 1 FROM excluded WHERE COL = UPPER(f.key))
),
-- Flatten target
target_flat AS (
    SELECT 
        t.join_key,
        f.key AS col_name,
        f.value::VARCHAR AS col_value
    FROM target_data t,
    LATERAL FLATTEN(input => t.all_cols) f
    WHERE UPPER(f.key) != UPPER($JOIN_KEY)
      AND NOT EXISTS (SELECT 1 FROM excluded WHERE COL = UPPER(f.key))
),
-- Compare at row level
row_status AS (
    SELECT 
        s.join_key,
        CASE 
            WHEN t.join_key IS NULL THEN 'MISSING_IN_TARGET'
            WHEN COUNT(CASE WHEN s.col_value IS DISTINCT FROM t.col_value THEN 1 END) > 0 THEN 'MISMATCH'
            ELSE 'MATCH'
        END AS status
    FROM source_flat s
    LEFT JOIN target_flat t 
        ON s.join_key = t.join_key 
        AND UPPER(s.col_name) = UPPER(t.col_name)
    GROUP BY s.join_key, t.join_key
)
SELECT 
    status,
    COUNT(*) AS row_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct
FROM row_status
GROUP BY status
ORDER BY 
    CASE status 
        WHEN 'MATCH' THEN 1 
        WHEN 'MISMATCH' THEN 2 
        ELSE 3 
    END;


-- ============================================================================
-- SECTION 4: Column-Level Match Statistics
-- ============================================================================
/*
Shows match percentage for each column - helps identify problematic columns.
*/

WITH excluded AS (
    SELECT UPPER(TRIM(VALUE)) AS COL 
    FROM TABLE(SPLIT_TO_TABLE($EXCLUDE_COLS, ',')) 
    WHERE TRIM(VALUE) != ''
),
source_data AS (
    SELECT 
        GET(OBJECT_CONSTRUCT(*), $JOIN_KEY)::VARCHAR AS join_key,
        OBJECT_CONSTRUCT(*) AS all_cols
    FROM IDENTIFIER($SOURCE_FQ)
    SAMPLE ($SAMPLE_SIZE ROWS)
),
target_data AS (
    SELECT 
        GET(OBJECT_CONSTRUCT(*), $JOIN_KEY)::VARCHAR AS join_key,
        OBJECT_CONSTRUCT(*) AS all_cols
    FROM IDENTIFIER($TARGET_FQ)
),
source_flat AS (
    SELECT 
        s.join_key,
        f.key AS col_name,
        f.value::VARCHAR AS col_value
    FROM source_data s,
    LATERAL FLATTEN(input => s.all_cols) f
    WHERE UPPER(f.key) != UPPER($JOIN_KEY)
      AND NOT EXISTS (SELECT 1 FROM excluded WHERE COL = UPPER(f.key))
),
target_flat AS (
    SELECT 
        t.join_key,
        f.key AS col_name,
        f.value::VARCHAR AS col_value
    FROM target_data t,
    LATERAL FLATTEN(input => t.all_cols) f
    WHERE UPPER(f.key) != UPPER($JOIN_KEY)
      AND NOT EXISTS (SELECT 1 FROM excluded WHERE COL = UPPER(f.key))
),
compared AS (
    SELECT 
        s.col_name,
        s.join_key,
        CASE 
            WHEN t.join_key IS NULL THEN 'ROW_MISSING'
            WHEN s.col_value IS NOT DISTINCT FROM t.col_value THEN 'MATCH'
            ELSE 'MISMATCH'
        END AS status
    FROM source_flat s
    LEFT JOIN target_flat t 
        ON s.join_key = t.join_key 
        AND UPPER(s.col_name) = UPPER(t.col_name)
)
SELECT 
    col_name AS COLUMN_NAME,
    COUNT(*) AS TOTAL_COMPARED,
    SUM(CASE WHEN status = 'MATCH' THEN 1 ELSE 0 END) AS MATCHES,
    SUM(CASE WHEN status = 'MISMATCH' THEN 1 ELSE 0 END) AS MISMATCHES,
    SUM(CASE WHEN status = 'ROW_MISSING' THEN 1 ELSE 0 END) AS ROW_MISSING,
    ROUND(SUM(CASE WHEN status = 'MATCH' THEN 1 ELSE 0 END) * 100.0 / 
          NULLIF(SUM(CASE WHEN status != 'ROW_MISSING' THEN 1 ELSE 0 END), 0), 2) AS MATCH_PCT
FROM compared
GROUP BY col_name
ORDER BY MISMATCHES DESC, col_name;


-- ============================================================================
-- SECTION 5: Detailed Mismatches (Show Actual Values)
-- ============================================================================
/*
Shows specific rows and columns that don't match, with actual values.
Limited to 100 rows.
*/

WITH excluded AS (
    SELECT UPPER(TRIM(VALUE)) AS COL 
    FROM TABLE(SPLIT_TO_TABLE($EXCLUDE_COLS, ',')) 
    WHERE TRIM(VALUE) != ''
),
source_data AS (
    SELECT 
        GET(OBJECT_CONSTRUCT(*), $JOIN_KEY)::VARCHAR AS join_key,
        OBJECT_CONSTRUCT(*) AS all_cols
    FROM IDENTIFIER($SOURCE_FQ)
    SAMPLE ($SAMPLE_SIZE ROWS)
),
target_data AS (
    SELECT 
        GET(OBJECT_CONSTRUCT(*), $JOIN_KEY)::VARCHAR AS join_key,
        OBJECT_CONSTRUCT(*) AS all_cols
    FROM IDENTIFIER($TARGET_FQ)
),
source_flat AS (
    SELECT 
        s.join_key,
        f.key AS col_name,
        f.value::VARCHAR AS col_value
    FROM source_data s,
    LATERAL FLATTEN(input => s.all_cols) f
    WHERE UPPER(f.key) != UPPER($JOIN_KEY)
      AND NOT EXISTS (SELECT 1 FROM excluded WHERE COL = UPPER(f.key))
),
target_flat AS (
    SELECT 
        t.join_key,
        f.key AS col_name,
        f.value::VARCHAR AS col_value
    FROM target_data t,
    LATERAL FLATTEN(input => t.all_cols) f
    WHERE UPPER(f.key) != UPPER($JOIN_KEY)
      AND NOT EXISTS (SELECT 1 FROM excluded WHERE COL = UPPER(f.key))
)
SELECT 
    s.join_key AS JOIN_KEY,
    s.col_name AS COLUMN_NAME,
    s.col_value AS SOURCE_VALUE,
    t.col_value AS TARGET_VALUE
FROM source_flat s
LEFT JOIN target_flat t 
    ON s.join_key = t.join_key 
    AND UPPER(s.col_name) = UPPER(t.col_name)
WHERE s.col_value IS DISTINCT FROM t.col_value
ORDER BY s.col_name, s.join_key
LIMIT 100;


-- ============================================================================
-- SECTION 6: Orphan Records - In Source but not Target
-- ============================================================================

WITH source_keys AS (
    SELECT DISTINCT GET(OBJECT_CONSTRUCT(*), $JOIN_KEY)::VARCHAR AS join_key
    FROM IDENTIFIER($SOURCE_FQ)
    SAMPLE ($SAMPLE_SIZE ROWS)
),
target_keys AS (
    SELECT DISTINCT GET(OBJECT_CONSTRUCT(*), $JOIN_KEY)::VARCHAR AS join_key
    FROM IDENTIFIER($TARGET_FQ)
)
SELECT 
    s.join_key AS JOIN_KEY,
    'MISSING IN TARGET' AS STATUS
FROM source_keys s
LEFT JOIN target_keys t ON s.join_key = t.join_key
WHERE t.join_key IS NULL
LIMIT 100;


-- ============================================================================
-- SECTION 6B: Full Source Rows Missing in Target (Dump All Columns)
-- ============================================================================
/*
Returns complete source rows that don't exist in target.
Good for investigating why rows are missing or for inserting into target.
*/

WITH source_keys AS (
    SELECT 
        GET(OBJECT_CONSTRUCT(*), $JOIN_KEY)::VARCHAR AS join_key,
        src.*
    FROM IDENTIFIER($SOURCE_FQ) src
),
target_keys AS (
    SELECT 
        GET(OBJECT_CONSTRUCT(*), $JOIN_KEY)::VARCHAR AS join_key,
        tgt.*
    FROM IDENTIFIER($TARGET_FQ) tgt
)
SELECT s.*
FROM source_keys s
LEFT JOIN target_keys t ON s.join_key = t.join_key
WHERE t.join_key IS NULL;


-- ============================================================================
-- SECTION 7: Orphan Records - In Target but not Source  
-- ============================================================================

WITH source_keys AS (
    SELECT DISTINCT GET(OBJECT_CONSTRUCT(*), $JOIN_KEY)::VARCHAR AS join_key
    FROM IDENTIFIER($SOURCE_FQ)
),
target_keys AS (
    SELECT DISTINCT GET(OBJECT_CONSTRUCT(*), $JOIN_KEY)::VARCHAR AS join_key
    FROM IDENTIFIER($TARGET_FQ)
    SAMPLE ($SAMPLE_SIZE ROWS)
)
SELECT 
    t.join_key AS JOIN_KEY,
    'MISSING IN SOURCE' AS STATUS
FROM target_keys t
LEFT JOIN source_keys s ON s.join_key = t.join_key
WHERE s.join_key IS NULL
LIMIT 100;


-- ============================================================================
-- SECTION 8: JSON Summary for PR
-- ============================================================================

WITH excluded AS (
    SELECT UPPER(TRIM(VALUE)) AS COL 
    FROM TABLE(SPLIT_TO_TABLE($EXCLUDE_COLS, ',')) 
    WHERE TRIM(VALUE) != ''
),
source_data AS (
    SELECT 
        GET(OBJECT_CONSTRUCT(*), $JOIN_KEY)::VARCHAR AS join_key,
        OBJECT_CONSTRUCT(*) AS all_cols
    FROM IDENTIFIER($SOURCE_FQ)
    SAMPLE ($SAMPLE_SIZE ROWS)
),
target_data AS (
    SELECT 
        GET(OBJECT_CONSTRUCT(*), $JOIN_KEY)::VARCHAR AS join_key,
        OBJECT_CONSTRUCT(*) AS all_cols
    FROM IDENTIFIER($TARGET_FQ)
),
source_flat AS (
    SELECT 
        s.join_key,
        f.key AS col_name,
        f.value::VARCHAR AS col_value
    FROM source_data s,
    LATERAL FLATTEN(input => s.all_cols) f
    WHERE UPPER(f.key) != UPPER($JOIN_KEY)
      AND NOT EXISTS (SELECT 1 FROM excluded WHERE COL = UPPER(f.key))
),
target_flat AS (
    SELECT 
        t.join_key,
        f.key AS col_name,
        f.value::VARCHAR AS col_value
    FROM target_data t,
    LATERAL FLATTEN(input => t.all_cols) f
    WHERE UPPER(f.key) != UPPER($JOIN_KEY)
      AND NOT EXISTS (SELECT 1 FROM excluded WHERE COL = UPPER(f.key))
),
compared AS (
    SELECT 
        s.join_key,
        s.col_name,
        CASE 
            WHEN t.join_key IS NULL THEN 'ROW_MISSING'
            WHEN s.col_value IS NOT DISTINCT FROM t.col_value THEN 'MATCH'
            ELSE 'MISMATCH'
        END AS status
    FROM source_flat s
    LEFT JOIN target_flat t 
        ON s.join_key = t.join_key 
        AND UPPER(s.col_name) = UPPER(t.col_name)
),
row_summary AS (
    SELECT 
        COUNT(DISTINCT join_key) AS total_rows,
        COUNT(DISTINCT CASE WHEN status = 'ROW_MISSING' THEN join_key END) AS missing_rows
    FROM compared
),
fully_matched_rows AS (
    SELECT COUNT(DISTINCT join_key) AS cnt
    FROM compared
    WHERE join_key NOT IN (
        SELECT DISTINCT join_key FROM compared WHERE status IN ('MISMATCH', 'ROW_MISSING')
    )
),
col_mismatches AS (
    SELECT col_name, COUNT(*) AS mismatch_count
    FROM compared
    WHERE status = 'MISMATCH'
    GROUP BY col_name
)
SELECT OBJECT_CONSTRUCT(
    'table_name', $TABLE_NAME,
    'comparison_type', 'DATA_VAULT_TEMP vs INFO_MART',
    'source', $SOURCE_FQ,
    'target', $TARGET_FQ,
    'sample_size', $SAMPLE_SIZE,
    'columns_compared', (SELECT COUNT(DISTINCT col_name) FROM compared),
    'overall_status', CASE 
        WHEN (SELECT cnt FROM fully_matched_rows) = (SELECT total_rows FROM row_summary) THEN '[PASS] ALL ROWS MATCH'
        WHEN (SELECT missing_rows FROM row_summary) > 0 THEN '[FAIL] MISSING ROWS'
        ELSE '[FAIL] DATA MISMATCHES'
    END,
    'row_stats', OBJECT_CONSTRUCT(
        'sampled', (SELECT total_rows FROM row_summary),
        'found_in_target', (SELECT total_rows - missing_rows FROM row_summary),
        'missing_in_target', (SELECT missing_rows FROM row_summary),
        'fully_matched', (SELECT cnt FROM fully_matched_rows),
        'with_differences', (SELECT total_rows - missing_rows FROM row_summary) - (SELECT cnt FROM fully_matched_rows),
        'match_pct', ROUND((SELECT cnt FROM fully_matched_rows) * 100.0 / 
                          NULLIF((SELECT total_rows - missing_rows FROM row_summary), 0), 2)
    ),
    'column_mismatches', (
        SELECT ARRAY_AGG(OBJECT_CONSTRUCT('column', col_name, 'mismatches', mismatch_count)) 
        FROM col_mismatches
    )
) AS DATA_COMPARISON_JSON;


-------------



