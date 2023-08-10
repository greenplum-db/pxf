-- @description query01 for PXF HDFS Readable Sequence with int recordkey 
SET datestyle TO 'ISO, MDY';

SELECT * from pxf_recordkey_int_type_r ORDER BY num1;
