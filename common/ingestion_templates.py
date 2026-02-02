"""DDL and SQL helpers for the Ingestion Copilot experience."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple
import textwrap

INGESTION_ENVIRONMENTS = ["DEV", "TEST", "UAT", "PROD"]
ADHOC_ENVIRONMENTS = ["DEV", "TEST", "UAT"]
ADF_POLL_SCHEDULE = (
    "Azure Data Factory polls METADATA_CONFIG_TABLE_ELT_ADHOC every hour on the hour."
)

INGESTION_SOURCE_TYPES: Dict[str, str] = {
    "Flat File": "FILE",
    "SQL Server": "SQL",
    "Azure DB": "AZURE_DB",
    "Excel File": "EXCEL",
}

METADATA_PROCEDURE_PARAMS: List[str] = [
    "LOGICAL_NAME",
    "FILE_LANDING_SCHEMA",
    "FILE_WILDCARD_PATTERN",
    "FILE_TABLE_NAME",
    "FILE_EXTENSION",
    "SHEET_NAME",
    "SOURCE_TYPE",
    "ENABLED",
    "FILE_FORMAT",
    "HAS_HEADER",
    "DATABASE_NAME",
    "SCHEMA_NAME",
    "SOURCE_TABLE_NAME",
    "DELTA_COLUMN",
    "CHANGE_TRACKING_TYPE",
    "SERVER_NAME",
    "FILE_SERVER_LOAD_TYPE",
    "FILESTAMP_FORMAT",
    "AUTO_INGEST",
    "FILE_SERVER_FILEPATH",
    "FIXED_WIDTH_FILETYPE",
    "REGEX_PATTERN",
]


@dataclass(frozen=True)
class DDLTemplateDefinition:
    """Represents a reusable DDL template."""

    key: str
    label: str
    description: str
    template: str
    environments: List[str]


def _dedent(sql: str) -> str:
    return textwrap.dedent(sql).strip()


DDL_TEMPLATES: List[DDLTemplateDefinition] = [
    DDLTemplateDefinition(
        key="azure_tables",
        label="Azure DB Source Dictionary",
        description="Dictionary of Azure SQL source tables (per environment).",
        template=_dedent(
            """
            CREATE OR REPLACE TABLE DATA_VAULT_TEMP.AZURE_TABLES__ENV__ (
                LOGICAL_NAME VARCHAR,
                SERVER_NAME VARCHAR,
                DATABASE_NAME VARCHAR,
                SCHEMA_NAME VARCHAR,
                SOURCE_TABLE_NAME VARCHAR,

                SOURCE_TYPE VARCHAR DEFAULT 'AZURE_DB',
                CREATED_DT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UPDATED_DT TIMESTAMP
            )
            COMMENT = 'Dictionary of all Azure DB source tables (__ENV__). Used for ingestion discovery and source-to-target validation.';
            """
        ),
        environments=INGESTION_ENVIRONMENTS,
    ),
    DDLTemplateDefinition(
        key="sql_server_tables",
        label="SQL Server Source Dictionary",
        description="Dictionary of SQL Server source tables (per environment).",
        template=_dedent(
            """
            CREATE OR REPLACE TABLE DATA_VAULT_TEMP.SQL_SERVER_TABLES__ENV__ (
                LOGICAL_NAME VARCHAR,
                SERVER_NAME VARCHAR,
                DATABASE_NAME VARCHAR,
                SCHEMA_NAME VARCHAR,
                SOURCE_TABLE_NAME VARCHAR,

                SOURCE_TYPE VARCHAR DEFAULT 'SQL_SERVER',
                CREATED_DT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UPDATED_DT TIMESTAMP
            )
            COMMENT = 'Dictionary of all SQL Server source tables (__ENV__). Used for ingestion discovery and source-to-target validation.';
            """
        ),
        environments=INGESTION_ENVIRONMENTS,
    ),
    DDLTemplateDefinition(
        key="excel_files",
        label="Excel File Dictionary",
        description="Dictionary of Excel-based sources (per environment).",
        template=_dedent(
            """
            CREATE OR REPLACE TABLE DATA_VAULT_TEMP.EXCEL_FILES__ENV__ (
                LOGICAL_NAME VARCHAR,
                FILE_CONTAINER VARCHAR,
                FILE_BUCKET VARCHAR,
                FILE_PATH_FROM_BUCKET VARCHAR,
                FILE_TYPE VARCHAR,
                FILE_WILDCARD_PATTERN VARCHAR,
                FILE_EXTENSION VARCHAR,
                SHEET_NAME VARCHAR,
                FILE_FORMAT VARCHAR,
                SOURCE_TYPE VARCHAR DEFAULT 'EXCEL',
                CREATED_DT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UPDATED_DT TIMESTAMP
            )
            COMMENT = 'Dictionary of all Excel file-based sources (__ENV__). Used for file discovery, validation, and compatibility views.';
            """
        ),
        environments=INGESTION_ENVIRONMENTS,
    ),
    DDLTemplateDefinition(
        key="edw_tables",
        label="EDW Table Dictionary",
        description="Dictionary of Enterprise Data Warehouse tables (per environment).",
        template=_dedent(
            """
            CREATE OR REPLACE TABLE DATA_VAULT_TEMP.EDW_TABLES__ENV__ (
                LOGICAL_NAME VARCHAR,
                DATABASE_NAME VARCHAR,
                SCHEMA_NAME VARCHAR,
                SOURCE_TABLE_NAME VARCHAR,
                SUBJECT_AREA VARCHAR,

                SOURCE_TYPE VARCHAR DEFAULT 'EDW',
                CREATED_DT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UPDATED_DT TIMESTAMP
            )
            COMMENT = 'Dictionary of all EDW tables (__ENV__). Used for ingestion discovery and lineage validation.';
            """
        ),
        environments=INGESTION_ENVIRONMENTS,
    ),
    DDLTemplateDefinition(
        key="source_files",
        label="Flat File Dictionary",
        description="Dictionary of generic flat-file sources (per environment).",
        template=_dedent(
            """
            CREATE OR REPLACE TABLE DATA_VAULT_TEMP.SOURCE_FILES__ENV__ (
                LOGICAL_NAME VARCHAR,
                FILE_CONTAINER VARCHAR,
                FILE_BUCKET VARCHAR,
                FILE_PATH_FROM_BUCKET VARCHAR,
                FILE_TYPE VARCHAR,
                FILE_WILDCARD_PATTERN VARCHAR,
                FILE_EXTENSION VARCHAR,
                FILE_FORMAT VARCHAR,
                SOURCE_TYPE VARCHAR DEFAULT 'FILE',
                CREATED_DT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UPDATED_DT TIMESTAMP
            )
            COMMENT = 'Dictionary of all flat-file sources (__ENV__). Used for file discovery, validation, and compatibility views.';
            """
        ),
        environments=INGESTION_ENVIRONMENTS,
    ),
    DDLTemplateDefinition(
        key="metadata_config_adhoc",
        label="Metadata Config (Adhoc)",
        description="Environment-specific adhoc metadata config table (not deployed to PROD).",
        template=_dedent(
            """
            CREATE OR REPLACE TABLE STAGE__ENV__.ELT.METADATA_CONFIG_TABLE_ELT_ADHOC (
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
            )
            COMMENT = 'Snowflake table for storing adhoc ingest metadata (__ENV__). ADF polls this table every hour to orchestrate on-demand loads.';
            """
        ),
        environments=ADHOC_ENVIRONMENTS,
    ),
    DDLTemplateDefinition(
        key="create_schema_dynamic",
        label="CREATE_SCHEMA_DYNAMIC Procedure",
        description="Utility procedure for provisioning STAGE schemas with role grants.",
        template=_dedent(
            """
            CREATE OR REPLACE PROCEDURE STAGE_DEV.ELT.CREATE_SCHEMA_DYNAMIC("ENVIRONMENT" VARCHAR, "DB_NAME" VARCHAR, "SCHEMA_NAME" VARCHAR)
            RETURNS VARCHAR
            LANGUAGE PYTHON
            RUNTIME_VERSION = '3.11'
            PACKAGES = ('snowflake-snowpark-python')
            HANDLER = 'main'
            EXECUTE AS CALLER
            AS '
            def create_schema_with_grants(snowpark_session, environment, db_name, schema_name):
                #Set up variables
                db_name = f"{db_name}_{environment}"
                ea_sysadmin_role_name = f"EA_{environment}_SYSADMIN"
                ar_stage_full = f"AR_{db_name}_{schema_name}_FULL"
                ar_stage_read = f"AR_{db_name}_{schema_name}_READ"
                ar_stage_write = f"AR_{db_name}_{schema_name}_WRITE"
                fr_svc_loader = f"FR_SVC_LOADER_{''PROD'' if environment == ''PROD'' else ''NONPROD''}"

                # Create a list of SQL statements to execute
                sql_statements = [
                    f"USE ROLE {ea_sysadmin_role_name}",
                    f"USE DATABASE {db_name}",
                    f"CREATE SCHEMA IF NOT EXISTS {schema_name} WITH MANAGED ACCESS",
                    f"CREATE DATABASE ROLE IF NOT EXISTS {ar_stage_full}",
                    f"CREATE DATABASE ROLE IF NOT EXISTS {ar_stage_read}",
                    f"CREATE DATABASE ROLE IF NOT EXISTS {ar_stage_write}",
                    f"GRANT DATABASE ROLE {ar_stage_full} TO ROLE {ea_sysadmin_role_name}",
                    f"GRANT USAGE, MONITOR ON DATABASE {db_name} TO DATABASE ROLE {ar_stage_full}",
                    f"GRANT USAGE, MONITOR ON SCHEMA {schema_name} TO DATABASE ROLE {ar_stage_full}",
                    f"GRANT CREATE TABLE, CREATE VIEW, CREATE MATERIALIZED VIEW, CREATE STREAM, CREATE FUNCTION, CREATE PROCEDURE, CREATE SEQUENCE, CREATE TASK, CREATE FILE FORMAT, CREATE STAGE, CREATE EXTERNAL TABLE, CREATE PIPE, CREATE DYNAMIC TABLE, CREATE MATERIALIZED VIEW, CREATE STREAMLIT, CREATE ALERT, CREATE TAG, CREATE MASKING POLICY, CREATE ROW ACCESS POLICY ON SCHEMA {schema_name} TO DATABASE ROLE {ar_stage_full}",

                    # Ownership grants
                    f"GRANT OWNERSHIP ON ALL TABLES IN SCHEMA {schema_name} TO DATABASE ROLE {ar_stage_full} REVOKE CURRENT GRANTS",
                    f"GRANT OWNERSHIP ON ALL EXTERNAL TABLES IN SCHEMA {schema_name} TO DATABASE ROLE {ar_stage_full} REVOKE CURRENT GRANTS",
                    f"GRANT OWNERSHIP ON ALL VIEWS IN SCHEMA {schema_name} TO DATABASE ROLE {ar_stage_full} REVOKE CURRENT GRANTS",
                    f"GRANT OWNERSHIP ON ALL STAGES IN SCHEMA {schema_name} TO DATABASE ROLE {ar_stage_full} REVOKE CURRENT GRANTS",
                    f"GRANT OWNERSHIP ON ALL FILE FORMATS IN SCHEMA {schema_name} TO DATABASE ROLE {ar_stage_full} REVOKE CURRENT GRANTS",
                    f"GRANT OWNERSHIP ON ALL STREAMS IN SCHEMA {schema_name} TO DATABASE ROLE {ar_stage_full} REVOKE CURRENT GRANTS",
                    f"GRANT OWNERSHIP ON ALL PROCEDURES IN SCHEMA {schema_name} TO DATABASE ROLE {ar_stage_full} REVOKE CURRENT GRANTS",
                    f"GRANT OWNERSHIP ON ALL FUNCTIONS IN SCHEMA {schema_name} TO DATABASE ROLE {ar_stage_full} REVOKE CURRENT GRANTS",
                    f"GRANT OWNERSHIP ON ALL SEQUENCES IN SCHEMA {schema_name} TO DATABASE ROLE {ar_stage_full} REVOKE CURRENT GRANTS",

                    # Subordinate role grants
                    f"GRANT DATABASE ROLE {ar_stage_read} TO DATABASE ROLE {ar_stage_full}",
                    f"GRANT DATABASE ROLE {ar_stage_write} TO DATABASE ROLE {ar_stage_full}",

                    # READ role privileges
                    f"GRANT USAGE, MONITOR ON DATABASE {db_name} TO DATABASE ROLE {ar_stage_read}",
                    f"GRANT USAGE, MONITOR ON SCHEMA {schema_name} TO DATABASE ROLE {ar_stage_read}",
                    f"GRANT SELECT, REFERENCES ON ALL TABLES IN SCHEMA {schema_name} TO DATABASE ROLE {ar_stage_read}",
                    f"GRANT SELECT, REFERENCES ON ALL VIEWS IN SCHEMA {schema_name} TO DATABASE ROLE {ar_stage_read}",
                    f"GRANT SELECT ON ALL EXTERNAL TABLES IN SCHEMA {schema_name} TO DATABASE ROLE {ar_stage_read}",
                    f"GRANT SELECT ON ALL DYNAMIC TABLES IN SCHEMA {schema_name} TO DATABASE ROLE {ar_stage_read}",
                    f"GRANT SELECT ON ALL MATERIALIZED VIEWS IN SCHEMA {schema_name} TO DATABASE ROLE {ar_stage_read}",

                    # WRITE role privileges
                    f"GRANT USAGE, MONITOR ON DATABASE {db_name} TO DATABASE ROLE {ar_stage_write}",
                    f"GRANT USAGE, MONITOR ON SCHEMA {schema_name} TO DATABASE ROLE {ar_stage_write}",
                    f"GRANT DELETE, INSERT, REFERENCES, TRUNCATE, UPDATE ON ALL TABLES IN SCHEMA {schema_name} TO DATABASE ROLE {ar_stage_write}",
                    f"GRANT SELECT ON ALL STREAMS IN SCHEMA {schema_name} TO DATABASE ROLE {ar_stage_write}",
                    f"GRANT USAGE, READ, WRITE ON ALL STAGES IN SCHEMA {schema_name} TO DATABASE ROLE {ar_stage_write}",
                    f"GRANT USAGE ON ALL FILE FORMATS IN SCHEMA {schema_name} TO DATABASE ROLE {ar_stage_write}",
                    f"GRANT USAGE ON ALL FUNCTIONS IN SCHEMA {schema_name} TO DATABASE ROLE {ar_stage_write}",
                    f"GRANT USAGE ON ALL SEQUENCES IN SCHEMA {schema_name} TO DATABASE ROLE {ar_stage_write}",
                    f"GRANT USAGE ON ALL PROCEDURES IN SCHEMA {schema_name} TO DATABASE ROLE {ar_stage_write}",
                    f"GRANT MONITOR, OPERATE ON ALL TASKS IN SCHEMA {schema_name} TO DATABASE ROLE {ar_stage_write}",
                    f"GRANT MONITOR, OPERATE ON ALL DYNAMIC TABLES IN SCHEMA {schema_name} TO DATABASE ROLE {ar_stage_write}",
                    f"GRANT MONITOR, OPERATE ON ALL ALERTS IN SCHEMA {schema_name} TO DATABASE ROLE {ar_stage_write}",

                    f"GRANT DATABASE ROLE {ar_stage_full} TO ROLE {fr_svc_loader}",

                    f"GRANT USAGE ON SCHEMA {db_name}.{schema_name} TO ROLE FR_ENGINEER_DATA_{environment}",
                    f"GRANT SELECT ON ALL TABLES IN SCHEMA {schema_name} TO ROLE FR_ENGINEER_DATA_{environment}",
                    f"GRANT SELECT ON FUTURE TABLES IN SCHEMA {schema_name} TO ROLE FR_ENGINEER_DATA_{environment}"
                ]

                # Execute all SQL statements
                for sql in sql_statements:
                    snowpark_session.sql(sql).collect()

            def main(snowpark_session, environment, db_name, schema_name):
                try:
                    # Create the original schema
                    create_schema_with_grants(snowpark_session, environment, db_name, schema_name)

                    # Create the landing schema
                    landing_schema_name = f"{schema_name}_LANDING"
                    create_schema_with_grants(snowpark_session, environment, db_name, landing_schema_name)

                    return f"Both {schema_name} and {schema_name}_LANDING schemas created successfully"
                except Exception as e:
                    return f"Error occurred: {str(e)}"
            ';
            """
        ),
        environments=["DEV"],
    ),
]

DDL_TEMPLATE_LOOKUP = {template.key: template for template in DDL_TEMPLATES}


def get_ddl_templates_for_env(env: str) -> List[DDLTemplateDefinition]:
    env_upper = env.upper()
    return [tpl for tpl in DDL_TEMPLATES if env_upper in tpl.environments]


def render_ddl_sql(key: str, env: str) -> str:
    env_upper = env.upper()
    template = DDL_TEMPLATE_LOOKUP[key]
    if env_upper not in template.environments:
        raise ValueError(f"Template {key} not available for {env_upper}")
    sql = template.template.replace("__ENV__", env_upper).replace("__env_lower__", env_upper.lower())
    return sql


def get_source_type_code(label: str) -> str:
    return INGESTION_SOURCE_TYPES[label]


def sql_literal(value: str | None) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, str):
        stripped = value.strip()
        if stripped == "":
            return "NULL"
        escaped = stripped.replace("'", "''")
        return f"'{escaped}'"
    return f"'{str(value)}'"


def sql_literal_allow_blank(value: str | None) -> str:
    """Return a SQL literal but keep empty strings as '' instead of NULL."""

    if value is None:
        return "''"
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def _metadata_assignments(
    *,
    logical_name: str,
    file_landing_schema: str,
    file_wildcard_pattern: str,
    file_table_name: str,
    file_extension: str,
    sheet_name: str,
    source_type_label: str,
    enabled_flag: bool,
    file_format: str,
    has_header: str,
    database_name: str,
    schema_name: str,
    source_table_name: str,
    delta_column: str,
    change_tracking_type: str,
    server_name: str,
    file_server_load_type: str,
    filestamp_format: str,
    auto_ingest: str,
    file_server_filepath: str,
    fixed_width_filetype: str,
    regex_pattern: str,
) -> List[Tuple[str, str]]:
    source_type_code = get_source_type_code(source_type_label)
    normalized_header = has_header.upper() if has_header else ""
    mapping = {
        "LOGICAL_NAME": sql_literal_allow_blank(logical_name),
        "FILE_LANDING_SCHEMA": sql_literal_allow_blank(file_landing_schema),
        "FILE_WILDCARD_PATTERN": sql_literal_allow_blank(file_wildcard_pattern),
        "FILE_TABLE_NAME": sql_literal_allow_blank(file_table_name),
        "FILE_EXTENSION": sql_literal_allow_blank(file_extension),
        "SHEET_NAME": sql_literal_allow_blank(sheet_name),
        "SOURCE_TYPE": sql_literal_allow_blank(source_type_code),
        "ENABLED": sql_literal_allow_blank("1" if enabled_flag else "0"),
        "FILE_FORMAT": sql_literal_allow_blank(file_format),
        "HAS_HEADER": sql_literal_allow_blank(normalized_header),
        "DATABASE_NAME": sql_literal_allow_blank(database_name),
        "SCHEMA_NAME": sql_literal_allow_blank(schema_name),
        "SOURCE_TABLE_NAME": sql_literal_allow_blank(source_table_name),
        "DELTA_COLUMN": sql_literal_allow_blank(delta_column),
        "CHANGE_TRACKING_TYPE": sql_literal_allow_blank(change_tracking_type),
        "SERVER_NAME": sql_literal_allow_blank(server_name),
        "FILE_SERVER_LOAD_TYPE": sql_literal_allow_blank(file_server_load_type),
        "FILESTAMP_FORMAT": sql_literal_allow_blank(filestamp_format),
        "AUTO_INGEST": sql_literal_allow_blank(auto_ingest),
        "FILE_SERVER_FILEPATH": sql_literal_allow_blank(file_server_filepath),
        "FIXED_WIDTH_FILETYPE": sql_literal_allow_blank(fixed_width_filetype),
        "REGEX_PATTERN": sql_literal_allow_blank(regex_pattern),
    }
    return [(param, mapping[param]) for param in METADATA_PROCEDURE_PARAMS]


def _build_metadata_procedure_sql(
    env_upper: str,
    *,
    schema_name: str,
    assignments: List[Tuple[str, str]],
    procedure_suffix: str,
    requested_by: str | None,
    notes: str | None,
    priority_flag: bool,
) -> str:
    role = f"EA_{env_upper}_SYSADMIN"
    database = f"STAGE_{env_upper}"
    warehouse = f"{database}_WH"
    schema_hint = (schema_name or "ELT").upper()
    preamble = _dedent(
        f"""
        USE ROLE {role};
        USE DATABASE {database};
        USE SCHEMA ELT;
        USE WAREHOUSE {warehouse};
        -- If you need a managed schema, uncomment:
        -- CALL {database}.ELT.CREATE_SCHEMA_DYNAMIC('{env_upper}', 'STAGE', '{schema_hint}');
        """
    ).strip()

    set_lines = "\n".join(
        f"SET {name:<25} = {value};" for name, value in assignments
    )

    lookup_sql = _dedent(
        f"""
        -- Check for existing metadata rows before calling the procedure
        SELECT *
        FROM {database}.ELT.METADATA_CONFIG_TABLE_ELT
        WHERE LOWER(TRIM(COALESCE(SERVER_NAME, ''))) = LOWER(TRIM($SERVER_NAME))
          AND LOWER(TRIM(COALESCE(SCHEMA_NAME, ''))) = LOWER(TRIM($SCHEMA_NAME))
          AND LOWER(TRIM(COALESCE(SOURCE_TABLE_NAME, ''))) = LOWER(TRIM($SOURCE_TABLE_NAME));
        """
    ).strip()

    annotations: List[str] = []
    if requested_by:
        annotations.append(f"-- Requested by: {requested_by}")
    if priority_flag:
        annotations.append("-- Priority flag: TRUE (expedite queue)")
    if notes:
        sanitized = notes.replace("\n", " ").strip()
        if sanitized:
            annotations.append(f"-- Notes: {sanitized}")
    annotation_block = "\n".join(annotations)

    procedure_name = f"{database}.ELT.{procedure_suffix}_{env_upper}"
    param_block = ",\n    ".join(f"${name}" for name, _ in assignments)
    call_block = _dedent(
        f"""
        CALL {procedure_name}(
            {param_block}
        );
        """
    ).strip()

    sections = [preamble, set_lines, lookup_sql]
    if annotation_block:
        sections.append(annotation_block)
    sections.append(call_block)
    return "\n\n".join(section for section in sections if section).strip()


def build_metadata_config_key(
    logical_name: str,
    source_table_name: str,
    environment: str,
    source_type_code: str,
) -> str:
    logical = (logical_name or "LOGICAL").upper()
    table = (source_table_name or logical_name or "SOURCE").upper()
    env_upper = environment.upper()
    return f"{env_upper}::{source_type_code}::{logical}::{table}"


def build_adhoc_insert_sql(
    environment: str,
    *,
    logical_name: str,
    file_landing_schema: str,
    file_wildcard_pattern: str,
    file_table_name: str,
    file_extension: str,
    sheet_name: str,
    source_type_label: str,
    enabled_flag: bool,
    file_format: str,
    has_header: str,
    database_name: str,
    schema_name: str,
    source_table_name: str,
    delta_column: str,
    change_tracking_type: str,
    server_name: str,
    file_server_load_type: str,
    filestamp_format: str,
    auto_ingest: str,
    file_server_filepath: str,
    fixed_width_filetype: str,
    regex_pattern: str,
    requested_by: str | None,
    notes: str | None,
    priority_flag: bool,
) -> str:
    env_upper = environment.upper()
    if env_upper not in ADHOC_ENVIRONMENTS:
        raise ValueError("Adhoc metadata table is only deployed to DEV/TEST/UAT")
    assignments = _metadata_assignments(
        logical_name=logical_name,
        file_landing_schema=file_landing_schema,
        file_wildcard_pattern=file_wildcard_pattern,
        file_table_name=file_table_name,
        file_extension=file_extension,
        sheet_name=sheet_name,
        source_type_label=source_type_label,
        enabled_flag=enabled_flag,
        file_format=file_format,
        has_header=has_header,
        database_name=database_name,
        schema_name=schema_name,
        source_table_name=source_table_name,
        delta_column=delta_column,
        change_tracking_type=change_tracking_type,
        server_name=server_name,
        file_server_load_type=file_server_load_type,
        filestamp_format=filestamp_format,
        auto_ingest=auto_ingest,
        file_server_filepath=file_server_filepath,
        fixed_width_filetype=fixed_width_filetype,
        regex_pattern=regex_pattern,
    )
    return _build_metadata_procedure_sql(
        env_upper,
        schema_name=schema_name,
        assignments=assignments,
        procedure_suffix="ADHOC_INSERT_METADATA_CONFIG_TABLE",
        requested_by=requested_by,
        notes=notes,
        priority_flag=priority_flag,
    )


def build_create_adhoc_proc_sql(environment: str) -> str:
    """Return CREATE OR REPLACE PROCEDURE SQL that inserts a single row into
    STAGE_<ENV>.ELT.METADATA_CONFIG_TABLE_ELT_ADHOC with the columns:
    METADATA_CONFIG_KEY, LOAD_START_DATETIME, LOAD_END_DATETIME, LAST_TRIGGER_TIMESTAMP.
    The procedure uses Python handler `run` and Snowpark to append the row.
    """
    env_upper = environment.upper()
    database = f"STAGE_{env_upper}"
    proc_name = f"{database}.ELT.INSERT_METADATA_CONFIG_TABLE_ADHOC_{env_upper}"
    table_name = f"{database}.ELT.METADATA_CONFIG_TABLE_ELT_ADHOC"

    py = f"""
import snowflake.snowpark as snowpark

def run(METADATA_CONFIG_KEY, LOAD_START_DATETIME, LOAD_END_DATETIME, LAST_TRIGGER_TIMESTAMP):
    session = snowpark.Session.builder.getOrCreate()
    insert_data = [(
        METADATA_CONFIG_KEY,
        LOAD_START_DATETIME,
        LOAD_END_DATETIME,
        LAST_TRIGGER_TIMESTAMP,
    )]
    df = session.create_dataframe(
        insert_data,
        schema=[
            "METADATA_CONFIG_KEY",
            "LOAD_START_DATETIME",
            "LOAD_END_DATETIME",
            "LAST_TRIGGER_TIMESTAMP",
        ],
    )
    df.write.mode("append").save_as_table("{table_name}")
    return "Inserted adhoc trigger"
""".strip()

    ddl = f"""
CREATE OR REPLACE PROCEDURE {proc_name}(
    METADATA_CONFIG_KEY VARCHAR,
    LOAD_START_DATETIME TIMESTAMP_NTZ,
    LOAD_END_DATETIME TIMESTAMP_NTZ,
    LAST_TRIGGER_TIMESTAMP TIMESTAMP_NTZ
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS CALLER
AS '
{py}
';
""".strip()

    return ddl


def build_schedule_sets_and_call(
    environment: str,
    *,
    logical_name: str,
    file_landing_schema: str,
    file_wildcard_pattern: str,
    file_table_name: str,
    file_extension: str,
    sheet_name: str,
    source_type_label: str,
    enabled_flag: bool,
    file_format: str,
    has_header: str,
    database_name: str,
    schema_name: str,
    source_table_name: str,
    delta_column: str,
    change_tracking_type: str,
    server_name: str,
    file_server_load_type: str,
    filestamp_format: str,
    auto_ingest: str,
    file_server_filepath: str,
    fixed_width_filetype: str,
    regex_pattern: str,
) -> Tuple[str, str]:
    """Return a tuple of (set_statements, call_statement) for the metadata procedure.

    `set_statements` contains SET lines for each procedure parameter (LOGICAL_NAME, ...).
    `call_statement` is the CALL to STAGE_<ENV>.ELT.INSERT_METADATA_CONFIG_TABLE_<ENV> with
    dollar-parameter placeholders (e.g. $LOGICAL_NAME).
    """
    env_upper = environment.upper()
    database = f"STAGE_{env_upper}"

    assignments = _metadata_assignments(
        logical_name=logical_name,
        file_landing_schema=file_landing_schema,
        file_wildcard_pattern=file_wildcard_pattern,
        file_table_name=file_table_name,
        file_extension=file_extension,
        sheet_name=sheet_name,
        source_type_label=source_type_label,
        enabled_flag=enabled_flag,
        file_format=file_format,
        has_header=has_header,
        database_name=database_name,
        schema_name=schema_name,
        source_table_name=source_table_name,
        delta_column=delta_column,
        change_tracking_type=change_tracking_type,
        server_name=server_name,
        file_server_load_type=file_server_load_type,
        filestamp_format=filestamp_format,
        auto_ingest=auto_ingest,
        file_server_filepath=file_server_filepath,
        fixed_width_filetype=fixed_width_filetype,
        regex_pattern=regex_pattern,
    )

    set_lines = "\n".join(f"SET {name:<25} = {value};" for name, value in assignments)

    procedure_name = f"{database}.ELT.INSERT_METADATA_CONFIG_TABLE_{env_upper}"
    param_block = ",\n".join(f"${name}" for name, _ in assignments)
    call_block = f"CALL {procedure_name}(\n    {param_block}\n);"

    return set_lines, call_block


def build_schedule_update_sql(
    environment: str,
    *,
    logical_name: str,
    file_landing_schema: str,
    file_wildcard_pattern: str,
    file_table_name: str,
    file_extension: str,
    sheet_name: str,
    source_type_label: str,
    enabled_flag: bool,
    file_format: str,
    has_header: str,
    database_name: str,
    schema_name: str,
    source_table_name: str,
    delta_column: str,
    change_tracking_type: str,
    server_name: str,
    file_server_load_type: str,
    filestamp_format: str,
    auto_ingest: str,
    file_server_filepath: str,
    fixed_width_filetype: str,
    regex_pattern: str,
    requested_by: str | None,
    notes: str | None,
    priority_flag: bool,
) -> str:
    env_upper = environment.upper()
    assignments = _metadata_assignments(
        logical_name=logical_name,
        file_landing_schema=file_landing_schema,
        file_wildcard_pattern=file_wildcard_pattern,
        file_table_name=file_table_name,
        file_extension=file_extension,
        sheet_name=sheet_name,
        source_type_label=source_type_label,
        enabled_flag=enabled_flag,
        file_format=file_format,
        has_header=has_header,
        database_name=database_name,
        schema_name=schema_name,
        source_table_name=source_table_name,
        delta_column=delta_column,
        change_tracking_type=change_tracking_type,
        server_name=server_name,
        file_server_load_type=file_server_load_type,
        filestamp_format=filestamp_format,
        auto_ingest=auto_ingest or "1",
        file_server_filepath=file_server_filepath,
        fixed_width_filetype=fixed_width_filetype,
        regex_pattern=regex_pattern,
    )
    return _build_metadata_procedure_sql(
        env_upper,
        schema_name=schema_name,
        assignments=assignments,
        procedure_suffix="INSERT_METADATA_CONFIG_TABLE",
        requested_by=requested_by,
        notes=notes,
        priority_flag=priority_flag,
    )
