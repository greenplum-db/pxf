-- @description query01 for PXF HDFS Readable error table

-- start_matchsubs
--                                                                                               
-- # create a match/subs
-- m/pxf:\/\/(.*)\/pxf_automation_data/
-- s/pxf:\/\/.*\/pxf_automation_data/pxf:\/\/pxf_automation_data/
--
-- end_matchsubs

SELECT * FROM err_table_test ORDER BY num ASC;

SELECT relname, filename, linenum, errmsg, rawdata FROM  gp_read_error_log('err_table_test') ORDER BY linenum ASC;
