-- @description query01 for writing Parquet List data with a valid schema file provided
SET bytea_output=hex;
SET datestyle TO 'ISO, MDY';

\pset null 'NIL'

SELECT * FROM parquet_list_user_provided_schema_on_hcfs_read ORDER BY id;
