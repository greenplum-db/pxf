-- @description query01 for PXF HDFS Readable Json with exceeding max size test cases

SELECT * from jsontest_max_size ORDER BY id;

SELECT * from jsontest_max_size_filefrag ORDER BY id;