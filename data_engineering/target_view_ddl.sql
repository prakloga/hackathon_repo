/*
Script: target_view_ddl.sql
Description: DDL statements to create the target view.
Team: Data Engineering
Date: 2026-01-10
Parameters: vsch - View Schema identifier (e.g., VIEW) | tsch - Target Schema identifier (e.g., DE))
*/
--!jinja
CREATE VIEW IF NOT EXISTS HACKATHON_DB.HACKATHON_{{vsch}}_SCH.TARGET_TABLE_VIEW
COMMENT = 'Created by Prakash Loganathan'
AS
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
,RANGE_MIN
,RANGE_MAX
,RANGE_MID
,HIRE_DATE
,WORK_CITY
,WORK_STATE
,WORK_ZIP
,EMPLOYEE_TYPE
,GENDER
,SUPERVISOR_INDICATOR
,DATA_LOAD_TS 
FROM HACKATHON_DB.HACKATHON_{{tsch}}_SCH.TARGET_TABLE
;