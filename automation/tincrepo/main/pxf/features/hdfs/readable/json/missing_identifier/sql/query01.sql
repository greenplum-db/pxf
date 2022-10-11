-- @description query01 for PXF HDFS Readable Json with missing identifier test cases

SELECT * from jsontest_missing_identifier ORDER BY id;

SELECT * from jsontest_missing_identifier_filefrag ORDER BY id;