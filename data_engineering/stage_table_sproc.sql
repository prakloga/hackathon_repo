/*
Script: stage_table_sproc.sql
Description: DDL statements to create the stage table SPROC.
Team: Data Engineering
Date: 2026-01-10
Parameters: tsch - Target Schema identifier (e.g., DE))
*/
CREATE PROCEDURE IF NOT EXISTS HACKATHON_DB.HACKATHON_{{tsch}}_SCH.STAGE_TABLE_SPROC(DB_NAME string, SCHEMA_NAME string, STG_SCHEMA_NAME string)
returns string not null
language python
runtime_version = '3.11'
handler = 'stage_table_process'
packages = ('snowflake-snowpark-python')
comment = 'Created by Prakash Loganathan'
log_level=info
execute as owner
as
$$
from snowflake.snowpark import Session
import logging

def stage_table_process(session: Session, db_name: str, schema_name: str, stg_schema_name: str) -> str:
    logger = logging.getLogger(f"stage_table_sproc")
    logger.info(f"Starting the stage table process...")

    try:
        conn = session.connection
        cur = conn.cursor()
        meta_sql = f"""
                    select
                     distinct
                     metadata$filename as filename
                    ,convert_timezone('UTC', 'America/Chicago', metadata$file_last_modified) as file_last_modified
                    from @{db_name}.{stg_schema_name}.SF_AZ_ENCRYPT_EXT_STG/employee_fake_data.csv.gpg
                    """
        cur.execute(meta_sql)
        query_id = cur.sfqid
        logger.info(f"Query executed successfully. Query ID: {query_id}")

        cur.execute(f"select * from table(result_scan('{query_id}'))")
        result = cur.fetchone()
        filename, file_last_modified = result
        logger.info(f"Source file name: {str(filename)}, Source file last modified: {str(file_last_modified)}")

        
        sql_stmt = f"""create or replace transient table {db_name}.{schema_name}.STAGE_TABLE
        AS
        SELECT
         {db_name}.{schema_name}.SPLIT_STRING_UDF(line, 1) as EMP_ID
        ,{db_name}.{schema_name}.SPLIT_STRING_UDF(line, 2) as LAST_NAME
        ,{db_name}.{schema_name}.SPLIT_STRING_UDF(line, 3) as FIRST_NAME
        ,{db_name}.{schema_name}.SPLIT_STRING_UDF(line, 4) as SUPERVISOR_ID
        ,{db_name}.{schema_name}.SPLIT_STRING_UDF(line, 5) as JOB_CODE
        ,{db_name}.{schema_name}.SPLIT_STRING_UDF(line, 6) as JOB_TITLE
        ,{db_name}.{schema_name}.SPLIT_STRING_UDF(line, 7) as MANAGEMENT_LEVE
        ,{db_name}.{schema_name}.SPLIT_STRING_UDF(line, 8) as SALES_INDICATOR
        ,{db_name}.{schema_name}.SPLIT_STRING_UDF(line, 9) as CURRENCY_CODE
        ,{db_name}.{schema_name}.SPLIT_STRING_UDF(line, 10) as RANGE_MIN
        ,{db_name}.{schema_name}.SPLIT_STRING_UDF(line, 11) as RANGE_MAX
        ,{db_name}.{schema_name}.SPLIT_STRING_UDF(line, 12) as RANGE_MID
        ,{db_name}.{schema_name}.SPLIT_STRING_UDF(line, 13) as HIRE_DATE
        ,{db_name}.{schema_name}.SPLIT_STRING_UDF(line, 14) as WORK_CITY
        ,{db_name}.{schema_name}.SPLIT_STRING_UDF(line, 15) as WORK_STATE
        ,{db_name}.{schema_name}.SPLIT_STRING_UDF(line, 16) as WORK_ZIP
        ,{db_name}.{schema_name}.SPLIT_STRING_UDF(line, 17) as EMPLOYEE_TYPE
        ,{db_name}.{schema_name}.SPLIT_STRING_UDF(line, 18) as GENDER
        ,{db_name}.{schema_name}.SPLIT_STRING_UDF(line, 19) as SUPERVISOR_INDICATOR
        ,current_timestamp() as data_load_ts
        FROM (SELECT 
         LINE
        ,row_number() over(order by seq4()) as row_num
        from table({db_name}.{schema_name}.GPG_DECRYPTION_UDTF(build_scoped_file_url(@{db_name}.{stg_schema_name}.SF_AZ_ENCRYPT_EXT_STG,    'employee_fake_data.csv.gpg')))
        )
        WHERE row_num > 1
        """
        cur.execute(sql_stmt)
        query_id = cur.sfqid
        logger.info(f"Query executed successfully. Query ID: {query_id}")

        cur.execute(f"select count(*) from {db_name}.{schema_name}.STAGE_TABLE")
        result = cur.fetchone()
        record_count = result[0]
        logger.info(f"Stage table record count: {str(record_count)}")
        return "SUCCESS"

    except Exception as e:
        logger.error(f"Error processing the SQL statements: {str(e)}")
        raise

    finally:
        cur.close()
        conn.close()
$$
;
