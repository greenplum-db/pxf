-- @description query01 for PXF HDFS Readable Json with malformed record test cases

SELECT * from jsontest_malformed_record_with_reject_limit ORDER BY id;
SELECT * from jsontest_malformed_record_with_reject_limit WHERE id IS NULL ORDER BY id;