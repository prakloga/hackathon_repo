/*
Script: target_table_sproc.sql
Description: DDL statements to create the target table SPROC.
Team: Data Engineering
Date: 2026-01-10
Parameters: tsch - Target Schema identifier (e.g., DE))
*/
CREATE PROCEDURE IF NOT EXISTS HACKATHON_DB.HACKATHON_{{tsch}}_SCH.TARGET_TABLE_SPROC(DB_NAME string, TGT_SCHEMA_NAME string, STG_SCHEMA_NAME string)
returns string not null
language python
runtime_version = '3.11'
handler = 'target_table_process'
packages = ('snowflake-snowpark-python')
comment = 'Created by Prakash Loganathan'
log_level=info
execute as owner
as
$$
from snowflake.snowpark import Session
import logging

def target_table_process(session: Session, db_name: str, tgt_schema_name: str, stg_schema_name: str) -> str:
    logger = logging.getLogger(f"target_table_sproc")
    logger.info(f"Starting the target table process...")

    try:
        conn = session.connection
        cur = conn.cursor()
       
        sql_stmt = f"""INSERT INTO {db_name}.{tgt_schema_name}.TARGET_TABLE
                    SELECT
                     EMP_ID
                    ,LAST_NAME
                    ,FIRST_NAME
                    ,SUPERVISOR_ID
                    ,JOB_CODE
                    ,JOB_TITLE
                    ,MANAGEMENT_LEVE
                    ,SALES_INDICATOR
                    ,CURRENCY_CODE
                    ,RANGE_MIN::number as RANGE_MIN
                    ,RANGE_MAX::number as RANGE_MAX
                    ,RANGE_MID::number as RANGE_MID
                    ,TO_DATE(HIRE_DATE, 'YYYY-MM-DD') as HIRE_DATE
                    ,WORK_CITY
                    ,WORK_STATE
                    ,WORK_ZIP
                    ,EMPLOYEE_TYPE
                    ,GENDER
                    ,SUPERVISOR_INDICATOR
                    ,DATA_LOAD_TS
                    FROM {db_name}.{stg_schema_name}.STAGE_TABLE
        """
        cur.execute(sql_stmt)
        query_id = cur.sfqid
        logger.info(f"Query executed successfully. Query ID: {query_id}")

        cur.execute(f"select * from table(result_scan('{query_id}'))")
        result = cur.fetchone()
        record_count = result[0]
        logger.info(f"Target table record count: {str(record_count)}")
        return "SUCCESS"

    except Exception as e:
        logger.error(f"Error processing the SQL statements: {str(e)}")
        raise

    finally:
        cur.close()
        conn.close()
$$
;