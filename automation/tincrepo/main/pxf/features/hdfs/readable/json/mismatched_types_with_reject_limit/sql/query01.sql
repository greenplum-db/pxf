-- @description query01 for PXF HDFS Readable Json supported primitive types test cases
-- start_matchsubs
-- m/^NOTICE:  found \d+ data formatting errors.*/
-- s/.*/NOTICE:  Found 6 data formatting errors (6 or more input rows). Rejected related input data./
-- end_matchsubs
SELECT *
FROM jsontest_mismatched_types_with_reject_limit
ORDER BY type_int;
SELECT relname, errmsg
FROM gp_read_error_log('jsontest_mismatched_types_with_reject_limit');
