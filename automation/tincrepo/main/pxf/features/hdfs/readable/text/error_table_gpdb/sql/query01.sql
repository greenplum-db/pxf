-- @description query01 for PXF HDFS Readable error table

-- start_matchsubs
--
-- # create a match/subs
--
-- # replace PXF URL with "pxf-location" string
-- m/pxf:\/\/(.*)\|/
-- s/pxf:\/\/.*data\?PROFILE=.+?\|/pxf-location\|/
--
-- m/\|.*\/data/
-- s/\|.*\/data/\|pxf-location/
--
-- end_matchsubs

SELECT * FROM err_table_test ORDER BY num ASC;

SELECT relname, filename, linenum, errmsg, rawdata FROM  gp_read_error_log('err_table_test') ORDER BY linenum ASC;
