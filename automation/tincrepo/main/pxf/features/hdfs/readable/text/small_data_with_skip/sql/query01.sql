-- @description query01 for PXF HDFS Readable Text Small Data cases
SET datestyle TO 'ISO, MDY';

SELECT * from pxf_hdfs_small_data_with_skip ORDER BY n1;
