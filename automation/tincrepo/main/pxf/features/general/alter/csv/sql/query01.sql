-- @description query01 test altering table, dropping columns and then adding them back

-- start_matchsubs
--
-- # create a match/subs
--
-- m/DETAIL/
-- s/DETAIL/CONTEXT/
--
-- m/ERROR/
-- s/ERROR:  missing data for column "bool"/ERROR:  invalid input syntax for integer: "1.0"/g
--
-- m/pxf:\/\/(.*)\/pxf_automation_data/
-- s/pxf:\/\/.*alter-tests.*/pxf:\/\/pxf_automation_data?PROFILE=*:text/
--
-- m/CONTEXT:.*line.*/
-- s/line \d* of //g
--
-- m/tmp\/pxf_automation_data\/(.*)/
-- s/tmp\/pxf_automation_data\/(.*)/pxf:\/\/pxf_automation_data?PROFILE=*:text/g
--
-- end_matchsubs

-- This query should error out with invalid input syntax for integer
SELECT * FROM pxf_alter_csv_table ORDER BY name;

-- Remove the column that does not exist
ALTER EXTERNAL TABLE pxf_alter_csv_table DROP COLUMN col_does_not_exist;

-- Run the query again and we get results back
SELECT * FROM pxf_alter_csv_table ORDER BY name;