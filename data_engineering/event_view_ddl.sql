/*
Script: gpg_decryption_udtf.sql
Description: DDL statements to create the GPG decryption UDTF.
Team: Data Engineering
Date: 2026-01-04
Parameters: csch - common Schema identifier (e.g., COMMON)
*/
CREATE SECURE VIEW IF NOT EXISTS HACKATHON_DB.HACKATHON_{{csch}}_SCH.HACKATHON_EVENT_TABLE_VIEW
AS
SELECT
 convert_timezone('UTC','America/Chicago', TIMESTAMP) as TIMESTAMP_CST
,START_TIMESTAMP
,convert_timezone('UTC','America/Chicago', OBSERVED_TIMESTAMP) as OBSERVED_TIMESTAMP_CST
,TRACE:span_id::string as trace_span_id
,TRACE:trace_id::string as trace_id
,RESOURCE
,RESOURCE_ATTRIBUTES:"db.user"::string as db_user
,RESOURCE_ATTRIBUTES:"snow.database.id"::string as snow_database_id
,RESOURCE_ATTRIBUTES:"snow.database.name"::string as snow_database_name
,RESOURCE_ATTRIBUTES:"snow.executable.id"::string as snow_executable_id
,RESOURCE_ATTRIBUTES:"snow.executable.name"::string as snow_executable_name
,RESOURCE_ATTRIBUTES:"snow.executable.runtime.version"::string as snow_executable_runtime_version
,RESOURCE_ATTRIBUTES:"snow.executable.type"::string as snow_executable_type
,RESOURCE_ATTRIBUTES:"snow.owner.id"::string as snow_owner_id
,RESOURCE_ATTRIBUTES:"snow.owner.name"::string as snow_owner_name
,RESOURCE_ATTRIBUTES:"snow.query.id"::string as snow_query_id
,RESOURCE_ATTRIBUTES:"snow.schema.id"::string as snow_schema_id
,RESOURCE_ATTRIBUTES:"snow.schema.name"::string as snow_schema_name
,RESOURCE_ATTRIBUTES:"snow.session.id"::string as snow_session_id
,RESOURCE_ATTRIBUTES:"snow.session.role.primary.id"::string as snow_session_role_primary_id
,RESOURCE_ATTRIBUTES:"snow.session.role.primary.name"::string as snow_session_role_primary_name 
,RESOURCE_ATTRIBUTES:"snow.user.id"::string as snow_user_id
,RESOURCE_ATTRIBUTES:"snow.warehouse.id"::string as snow_warehouse_id
,RESOURCE_ATTRIBUTES:"snow.warehouse.name"::string as snow_warehouse_name
,RESOURCE_ATTRIBUTES:"telemetry.sdk.language"::string as telemetry_sdk_language
,SCOPE:"name"::string as scope_name
,SCOPE_ATTRIBUTES
,RECORD_TYPE
,RECORD:"severity_number"::string as record_severity_number
,RECORD:"severity_text"::string as record_severity_text
,RECORD_ATTRIBUTES:"code.filepath"::string as code_filepath
,RECORD_ATTRIBUTES:"code.function"::string as code_function
,RECORD_ATTRIBUTES:"code.lineno"::string as code_lineno
,VALUE::string as VALUE
,EXEMPLARS
FROM HACKATHON_DB.HACKATHON_{{csch}}_SCH.HACKATHON_EVENT_TABLE
;