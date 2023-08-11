-- start_matchsubs
-- m/ +:/
-- s/( +):/\1|/
-- m/\+$/
-- s/\+$//
-- m/^NOTICE:  found \d+ data formatting errors.*/
-- s/.*/NOTICE:  Found 1 data formatting errors (1 or more input rows). Rejected related input data./
-- end_matchsubs
-- @description query01 for PXF HDFS Readable Json with malformed record test cases
SELECT *
FROM jsontest_malformed_record_with_reject_limit
ORDER BY id;
