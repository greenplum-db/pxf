-- @description query01 for PXF HDFS Readable Sequence with text recordkey 
SET datestyle TO 'ISO, MDY';

SELECT * from pxf_recordkey_text_type_r ORDER BY num1;
