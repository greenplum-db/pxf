-- @description query01 for writing timestamp with timezone ORC data types
\pset null 'NIL'
SET bytea_output=hex;

SET timezone='America/Los_Angeles';
SELECT * FROM orc_timestamp_with_timezone_readable ORDER BY id;

SET timezone='America/New_York';
SELECT * FROM orc_timestamp_with_timezone_readable ORDER BY id;
