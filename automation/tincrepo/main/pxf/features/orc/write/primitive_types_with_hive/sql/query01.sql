-- @description query01 for writing primitive ORC data types using the Hive profile
SET bytea_output=hex;
SET datestyle TO 'ISO, MDY';

SET timezone='America/Los_Angeles';
-- for the numeric column in GPDB, decimal(38,18) in Hive, there are differences in output between different hive versions.
-- make the print consistent by doing `round(c_numeric, 18) as c_numeric` to force the same precision.
SELECT id, c_bool, c_bytea, c_bigint, c_small, c_int, c_text, c_real, c_float, c_char, c_varchar, c_varchar_nolimit, c_date, c_time, c_timestamp, round(c_numeric, 18) as c_numeric, c_uuid FROM pxf_orc_primitive_types_with_hive_readable ORDER BY id;

SET timezone='America/New_York';
SELECT id, c_bool, c_bytea, c_bigint, c_small, c_int, c_text, c_real, c_float, c_char, c_varchar, c_varchar_nolimit, c_date, c_time, c_timestamp, round(c_numeric, 18) as c_numeric, c_uuid FROM pxf_orc_primitive_types_with_hive_readable ORDER BY id;
