-- @description query01 for PXF HDFS Readable Json supported primitive types test cases
SELECT *
FROM jsontest_mismatched_types_with_reject_limit
ORDER BY type_int;
SELECT relname, errmsg
FROM gp_read_error_log('jsontest_mismatched_types_with_reject_limit');