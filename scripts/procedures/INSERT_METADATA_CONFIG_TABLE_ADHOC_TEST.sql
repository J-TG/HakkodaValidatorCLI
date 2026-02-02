CREATE OR REPLACE PROCEDURE STAGE_TEST.ELT.INSERT_METADATA_CONFIG_TABLE_ADHOC_TEST(
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
    df.write.mode("append").save_as_table("STAGE_TEST.ELT.METADATA_CONFIG_TABLE_ELT_ADHOC")
    return "Inserted adhoc trigger"
';
