"""Canonical modelling validation SQL steps."""

from dataclasses import dataclass
from typing import List


@dataclass(frozen=True)
class ValidationQueryStep:
    key: str
    title: str
    description: str
    sql_template: str
    optional: bool = False


def get_modelling_validation_steps(include_json: bool) -> List[ValidationQueryStep]:
    """Return ordered modelling validation steps, optionally including JSON summary."""
    steps: List[ValidationQueryStep] = [
        ValidationQueryStep(
            key="column_inventory",
            title="Column Inventory",
            description="Compare column presence, datatypes, and actions between source and target schemas.",
            sql_template="""
WITH source_cols AS (
    SELECT COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION
    FROM {source_db}.INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = '{source_schema}' AND TABLE_NAME = '{table_name}'
),
target_cols AS (
    SELECT COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION
    FROM {target_db}.INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = '{target_schema}' AND TABLE_NAME = '{table_name}'
),
excluded AS (
    SELECT UPPER(TRIM(VALUE)) AS COL
    FROM TABLE(SPLIT_TO_TABLE('{exclude_cols}', ','))
    WHERE TRIM(VALUE) != ''
)
SELECT 
    COALESCE(s.COLUMN_NAME, t.COLUMN_NAME) AS COLUMN_NAME,
    CASE WHEN s.COLUMN_NAME IS NOT NULL THEN 'YES' ELSE 'NO' END AS IN_SOURCE,
    CASE WHEN t.COLUMN_NAME IS NOT NULL THEN 'YES' ELSE 'NO' END AS IN_TARGET,
    s.DATA_TYPE AS SOURCE_TYPE,
    t.DATA_TYPE AS TARGET_TYPE,
    CASE 
        WHEN UPPER(COALESCE(s.COLUMN_NAME, t.COLUMN_NAME)) = UPPER('{join_key}') THEN 'JOIN KEY'
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
        WHEN UPPER(COALESCE(s.COLUMN_NAME, t.COLUMN_NAME)) = UPPER('{join_key}') THEN 1
        WHEN s.COLUMN_NAME IS NOT NULL AND t.COLUMN_NAME IS NOT NULL AND ex.COL IS NULL THEN 2
        ELSE 3
    END,
    COALESCE(s.ORDINAL_POSITION, t.ORDINAL_POSITION, 999);
""",
        ),
        ValidationQueryStep(
            key="row_counts",
            title="Row Count & High Watermarks",
            description="Validate record counts and load timestamps between source and target tables.",
            sql_template="""
SELECT 
    'SOURCE' AS DATASET, 
    '{source_fq}' AS TABLE_NAME,
    COUNT(*) AS ROW_COUNT,
    MAX({source_high_watermark}) AS MAX_LOAD_DATE
FROM {source_fq}
UNION ALL
SELECT 
    'TARGET' AS DATASET, 
    '{target_fq}' AS TABLE_NAME,
    COUNT(*) AS ROW_COUNT, 
    MAX({target_high_watermark}) AS MAX_LOAD_DATE
FROM {target_fq};
""",
        ),
        ValidationQueryStep(
            key="row_level_summary",
            title="Row-Level Match Summary",
            description="Summarize MATCH/MISMATCH/MISSING counts using sampled source data flattened across columns.",
            sql_template="""
WITH excluded AS (
    SELECT UPPER(TRIM(VALUE)) AS COL 
    FROM TABLE(SPLIT_TO_TABLE('{exclude_cols}', ',')) 
    WHERE TRIM(VALUE) != ''
),
source_data AS (
    SELECT 
        GET(OBJECT_CONSTRUCT(*), '{join_key}')::VARCHAR AS join_key,
        OBJECT_CONSTRUCT(*) AS all_cols
    FROM {source_fq}
    SAMPLE ({sample_size} ROWS)
),
target_data AS (
    SELECT 
        GET(OBJECT_CONSTRUCT(*), '{join_key}')::VARCHAR AS join_key,
        OBJECT_CONSTRUCT(*) AS all_cols
    FROM {target_fq}
),
source_flat AS (
    SELECT 
        s.join_key,
        f.key AS col_name,
        f.value::VARCHAR AS col_value
    FROM source_data s,
    LATERAL FLATTEN(input => s.all_cols) f
    WHERE UPPER(f.key) != UPPER('{join_key}')
      AND NOT EXISTS (SELECT 1 FROM excluded WHERE COL = UPPER(f.key))
),
target_flat AS (
    SELECT 
        t.join_key,
        f.key AS col_name,
        f.value::VARCHAR AS col_value
    FROM target_data t,
    LATERAL FLATTEN(input => t.all_cols) f
    WHERE UPPER(f.key) != UPPER('{join_key}')
      AND NOT EXISTS (SELECT 1 FROM excluded WHERE COL = UPPER(f.key))
),
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
""",
        ),
        ValidationQueryStep(
            key="column_match_stats",
            title="Column-Level Match Stats",
            description="Return per-column match, mismatch, and row-missing counts for the sampled dataset.",
            sql_template="""
WITH excluded AS (
    SELECT UPPER(TRIM(VALUE)) AS COL 
    FROM TABLE(SPLIT_TO_TABLE('{exclude_cols}', ',')) 
    WHERE TRIM(VALUE) != ''
),
source_data AS (
    SELECT 
        GET(OBJECT_CONSTRUCT(*), '{join_key}')::VARCHAR AS join_key,
        OBJECT_CONSTRUCT(*) AS all_cols
    FROM {source_fq}
    SAMPLE ({sample_size} ROWS)
),
target_data AS (
    SELECT 
        GET(OBJECT_CONSTRUCT(*), '{join_key}')::VARCHAR AS join_key,
        OBJECT_CONSTRUCT(*) AS all_cols
    FROM {target_fq}
),
source_flat AS (
    SELECT 
        s.join_key,
        f.key AS col_name,
        f.value::VARCHAR AS col_value
    FROM source_data s,
    LATERAL FLATTEN(input => s.all_cols) f
    WHERE UPPER(f.key) != UPPER('{join_key}')
      AND NOT EXISTS (SELECT 1 FROM excluded WHERE COL = UPPER(f.key))
),
target_flat AS (
    SELECT 
        t.join_key,
        f.key AS col_name,
        f.value::VARCHAR AS col_value
    FROM target_data t,
    LATERAL FLATTEN(input => t.all_cols) f
    WHERE UPPER(f.key) != UPPER('{join_key}')
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
    ROUND(
        SUM(CASE WHEN status = 'MATCH' THEN 1 ELSE 0 END) * 100.0 / 
        NULLIF(SUM(CASE WHEN status != 'ROW_MISSING' THEN 1 ELSE 0 END), 0),
        2
    ) AS MATCH_PCT
FROM compared
GROUP BY col_name
ORDER BY MISMATCHES DESC, col_name;
""",
        ),
        ValidationQueryStep(
            key="detailed_mismatches",
            title="Detailed Mismatches",
            description="Display specific row/column mismatches between source sample and target data (limit 100).",
            sql_template="""
WITH excluded AS (
    SELECT UPPER(TRIM(VALUE)) AS COL 
    FROM TABLE(SPLIT_TO_TABLE('{exclude_cols}', ',')) 
    WHERE TRIM(VALUE) != ''
),
source_data AS (
    SELECT 
        GET(OBJECT_CONSTRUCT(*), '{join_key}')::VARCHAR AS join_key,
        OBJECT_CONSTRUCT(*) AS all_cols
    FROM {source_fq}
    SAMPLE ({sample_size} ROWS)
),
target_data AS (
    SELECT 
        GET(OBJECT_CONSTRUCT(*), '{join_key}')::VARCHAR AS join_key,
        OBJECT_CONSTRUCT(*) AS all_cols
    FROM {target_fq}
),
source_flat AS (
    SELECT 
        s.join_key,
        f.key AS col_name,
        f.value::VARCHAR AS col_value
    FROM source_data s,
    LATERAL FLATTEN(input => s.all_cols) f
    WHERE UPPER(f.key) != UPPER('{join_key}')
      AND NOT EXISTS (SELECT 1 FROM excluded WHERE COL = UPPER(f.key))
),
target_flat AS (
    SELECT 
        t.join_key,
        f.key AS col_name,
        f.value::VARCHAR AS col_value
    FROM target_data t,
    LATERAL FLATTEN(input => t.all_cols) f
    WHERE UPPER(f.key) != UPPER('{join_key}')
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
""",
        ),
        ValidationQueryStep(
            key="orphan_source_keys",
            title="Orphan Source Keys",
            description="List sampled source keys that were not found in the target table.",
            sql_template="""
WITH source_keys AS (
    SELECT DISTINCT GET(OBJECT_CONSTRUCT(*), '{join_key}')::VARCHAR AS join_key
    FROM {source_fq}
    SAMPLE ({sample_size} ROWS)
),
target_keys AS (
    SELECT DISTINCT GET(OBJECT_CONSTRUCT(*), '{join_key}')::VARCHAR AS join_key
    FROM {target_fq}
)
SELECT 
    s.join_key AS JOIN_KEY,
    'MISSING IN TARGET' AS STATUS
FROM source_keys s
LEFT JOIN target_keys t ON s.join_key = t.join_key
WHERE t.join_key IS NULL
LIMIT 100;
""",
        ),
        ValidationQueryStep(
            key="full_source_rows_missing",
            title="Source Rows Missing In Target (Full Payload)",
            description="Return complete source rows that do not exist in target for deeper troubleshooting.",
            sql_template="""
WITH source_keys AS (
    SELECT 
        GET(OBJECT_CONSTRUCT(*), '{join_key}')::VARCHAR AS join_key,
        src.*
    FROM {source_fq} src
),
target_keys AS (
    SELECT 
        GET(OBJECT_CONSTRUCT(*), '{join_key}')::VARCHAR AS join_key,
        tgt.*
    FROM {target_fq} tgt
)
SELECT s.*
FROM source_keys s
LEFT JOIN target_keys t ON s.join_key = t.join_key
WHERE t.join_key IS NULL;
""",
        ),
        ValidationQueryStep(
            key="orphan_target_keys",
            title="Orphan Target Keys",
            description="List sampled target keys that are missing in the source dataset.",
            sql_template="""
WITH source_keys AS (
    SELECT DISTINCT GET(OBJECT_CONSTRUCT(*), '{join_key}')::VARCHAR AS join_key
    FROM {source_fq}
),
target_keys AS (
    SELECT DISTINCT GET(OBJECT_CONSTRUCT(*), '{join_key}')::VARCHAR AS join_key
    FROM {target_fq}
    SAMPLE ({sample_size} ROWS)
)
SELECT 
    t.join_key AS JOIN_KEY,
    'MISSING IN SOURCE' AS STATUS
FROM target_keys t
LEFT JOIN source_keys s ON s.join_key = t.join_key
WHERE s.join_key IS NULL
LIMIT 100;
""",
        ),
    ]

    if include_json:
        steps.append(
            ValidationQueryStep(
                key="json_summary",
                title="JSON Summary",
                description="Generate a compact JSON payload summarizing the entire comparison.",
                optional=True,
                sql_template="""
WITH excluded AS (
    SELECT UPPER(TRIM(VALUE)) AS COL 
    FROM TABLE(SPLIT_TO_TABLE('{exclude_cols}', ',')) 
    WHERE TRIM(VALUE) != ''
),
source_data AS (
    SELECT 
        GET(OBJECT_CONSTRUCT(*), '{join_key}')::VARCHAR AS join_key,
        OBJECT_CONSTRUCT(*) AS all_cols
    FROM {source_fq}
    SAMPLE ({sample_size} ROWS)
),
target_data AS (
    SELECT 
        GET(OBJECT_CONSTRUCT(*), '{join_key}')::VARCHAR AS join_key,
        OBJECT_CONSTRUCT(*) AS all_cols
    FROM {target_fq}
),
source_flat AS (
    SELECT 
        s.join_key,
        f.key AS col_name,
        f.value::VARCHAR AS col_value
    FROM source_data s,
    LATERAL FLATTEN(input => s.all_cols) f
    WHERE UPPER(f.key) != UPPER('{join_key}')
      AND NOT EXISTS (SELECT 1 FROM excluded WHERE COL = UPPER(f.key))
),
target_flat AS (
    SELECT 
        t.join_key,
        f.key AS col_name,
        f.value::VARCHAR AS col_value
    FROM target_data t,
        LATERAL FLATTEN(input => t.all_cols) f
    WHERE UPPER(f.key) != UPPER('{join_key}')
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
    'table_name', '{table_name}',
    'comparison_type', 'DATA_VAULT_TEMP vs INFO_MART',
    'source', '{source_fq}',
    'target', '{target_fq}',
    'sample_size', {sample_size},
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
""",
            ),
        )

    return steps
