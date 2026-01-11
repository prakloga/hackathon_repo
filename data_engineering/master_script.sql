/*
Script: master_script.sql
Description: Master script to execute all DDL and DML object creation scripts.
Team: Data Engineering
Date: 2026-01-10
*/
!source data_engineering/gpg_decryption_udtf.sql;
!source data_engineering/split_string_udf.sql;
!source data_engineering/stage_table_sproc.sql;
!source data_engineering/target_table_ddl.sql;
!source data_engineering/target_view_ddl.sql;
!source data_engineering/target_table_sproc.sql;
!source data_engineering/root_task.sql;