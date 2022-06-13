-- @description query01 for writing primitive ORC data types with nulls
\pset null 'NIL'
SET bytea_output=hex;

SELECT * FROM orc_primitive_types_nulls_readable ORDER BY id;