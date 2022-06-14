-- @description query01 for writing primitive ORC data types with nulls
\pset null 'NIL'
SET bytea_output=hex;

SET timezone='America/Los_Angeles';
SELECT * FROM orc_primitive_types_nulls_readable ORDER BY id;

SET timezone='America/New_York';
SELECT * FROM orc_primitive_types_nulls_readable ORDER BY id;
