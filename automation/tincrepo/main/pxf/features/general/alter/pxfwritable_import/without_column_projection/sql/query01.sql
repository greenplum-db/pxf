-- @description query01 test altering table, dropping columns and then adding them back

-- start_matchsubs
--
-- # create a match/subs
--
-- m/DETAIL/
-- s/DETAIL/CONTEXT/
--
-- end_matchsubs

-- sets the bytea output to the expected by the tests
SET bytea_output='escape';

SELECT * from pxf_alter_avro_table ORDER BY type_int;

-- Drop one of the columns
ALTER EXTERNAL TABLE pxf_alter_avro_table DROP COLUMN col_does_not_exist;

SELECT * from pxf_alter_avro_table ORDER BY type_int;

-- Drop the last column and then add it back
ALTER EXTERNAL TABLE pxf_alter_avro_table DROP COLUMN type_boolean;
ALTER EXTERNAL TABLE pxf_alter_avro_table ADD COLUMN type_boolean BOOL;

-- Run the query again
SELECT * from pxf_alter_avro_table ORDER BY type_int;