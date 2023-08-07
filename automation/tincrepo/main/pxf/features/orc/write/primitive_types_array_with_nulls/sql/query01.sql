-- @description query01 for writing arrays of primitive ORC data types
\pset null 'NIL'
SET bytea_output=hex;
SET datestyle TO 'ISO, MDY';

SET timezone='America/Los_Angeles';
SELECT * FROM orc_primitive_arrays_readable ORDER BY id;

SET timezone='America/New_York';
SELECT * FROM orc_primitive_arrays_readable ORDER BY id;
