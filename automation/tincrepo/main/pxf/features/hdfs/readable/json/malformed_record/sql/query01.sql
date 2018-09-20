-- @description query01 for PXF HDFS Readable Json with malformed record test cases

SELECT * from jsontest_malformed_record ORDER BY id;
SELECT * from jsontest_malformed_record WHERE id IS NULL ORDER BY id;