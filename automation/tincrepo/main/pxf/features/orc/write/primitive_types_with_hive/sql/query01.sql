-- @description query01 for writing primitive ORC data types using the Hive profile
SET bytea_output=hex;

SET timezone='America/Los_Angeles';
-- for the numeric column in GPDB, decimal(38,18) in Hive, there are differences in output between different hive versions.
-- make the print consistent by doing `round(col13, 18) as col13` to force the same precision.
SELECT id, col00, col01, col02, col03, col04, col05, col06, col07, col08, col09, col10, col11, col12, round(col13, 18) as col13, col14 FROM orc_primitive_types_with_hive_readable ORDER BY id;

SET timezone='America/New_York';
SELECT id, col00, col01, col02, col03, col04, col05, col06, col07, col08, col09, col10, col11, col12, round(col13, 18) as col13, col14 FROM orc_primitive_types_with_hive_readable ORDER BY id;
