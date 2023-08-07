-- @description query01 for writing timestamp with timezone ORC data types
\pset null 'NIL'
SET bytea_output=hex;
SET datestyle TO 'ISO, MDY';

SET timezone='America/Los_Angeles';
SELECT * FROM pxf_orc_timestamp_with_timezone_readable ORDER BY id;

SET timezone='America/New_York';
SELECT * FROM pxf_orc_timestamp_with_timezone_readable ORDER BY id;
