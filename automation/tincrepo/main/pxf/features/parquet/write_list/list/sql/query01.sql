-- @description query01 for writing Parquet List data types (except timestamp List)
SET bytea_output=hex;
SET datestyle TO 'ISO, MDY';

\pset null 'NIL'

SELECT * FROM pxf_parquet_read_list ORDER BY id;
