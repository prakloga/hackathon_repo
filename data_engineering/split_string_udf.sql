/*
Script: gpg_decryption_udtf.sql
Description: DDL statements to create the GPG decryption UDTF.
Team: Data Engineering
Date: 2026-01-10
Parameters: csch - common Schema identifier (e.g., COMMON) | tsch - Target Schema identifier (e.g., DE))
*/
CREATE FUNCTION IF NOT EXISTS HACKATHON_DB.HACKATHON_{{tsch}}_SCH.SPLIT_STRING_UDF(line string, item_num integer)
returns string
comment = 'Created by Prakash Loganathan'
log_level = info
as
$$
split_part(line,',',item_num)
$$
;