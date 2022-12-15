-- @description query01 for Parquet List data types (except timestamp List)
SET bytea_output=hex;

\pset null 'NIL'

SELECT * FROM parquet_list_user_provided_schema_on_hcfs_read ORDER BY id;