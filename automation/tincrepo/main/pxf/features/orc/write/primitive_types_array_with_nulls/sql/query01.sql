-- @description query01 for writing arrays of primitive ORC data types
\pset null 'NIL'
SET bytea_output=hex;

SET extra_float_digits=0;

SET timezone='America/Los_Angeles';
SELECT * FROM orc_primitive_arrays_readable ORDER BY id;

SET timezone='America/New_York';
SELECT * FROM orc_primitive_arrays_readable ORDER BY id;
