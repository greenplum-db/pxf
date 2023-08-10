-- @description query01 for PXF HDFS Readable Sequence supported array types test cases
SET datestyle TO 'ISO, MDY';

SELECT * from writable_in_sequence ORDER BY num1;
