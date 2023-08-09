-- @description query01 for Parquet List data types (except timestamp List)
SET bytea_output=hex;
SET datestyle TO 'ISO, MDY';

\pset null 'NIL'

SELECT * FROM pxf_parquet_list_types ORDER BY id;
