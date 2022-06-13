-- @description query01 for writing primitive ORC data types
SET bytea_output=hex;

SELECT * FROM orc_primitive_types_readable ORDER BY id;
