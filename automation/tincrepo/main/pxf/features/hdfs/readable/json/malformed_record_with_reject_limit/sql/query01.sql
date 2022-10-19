-- start_matchsubs
-- m/ +:/
-- s/( +):/\1|/
-- m/\+$/
-- s/\+$//
-- end_matchsubs
-- @description query01 for PXF HDFS Readable Json with malformed record test cases
SELECT *
FROM jsontest_malformed_record_with_reject_limit
ORDER BY id;
SELECT relname, errmsg
FROM gp_read_error_log('jsontest_malformed_record_with_reject_limit');

SELECT *
FROM jsontest_malformed_record_with_reject_limit_filefrag
ORDER BY id;
SELECT relname, errmsg
FROM gp_read_error_log('jsontest_malformed_record_with_reject_limit_filefrag');