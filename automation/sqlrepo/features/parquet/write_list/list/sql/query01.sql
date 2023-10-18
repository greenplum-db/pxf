-- @description query01 for writing Parquet List data types (except timestamp List)
SET bytea_output=hex;

\pset null 'NIL'

SELECT * FROM pxf_parquet_read_list ORDER BY id;