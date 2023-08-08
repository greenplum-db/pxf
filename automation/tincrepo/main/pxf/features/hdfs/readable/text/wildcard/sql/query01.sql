-- @description query01 for PXF HDFS Readable wildcard location
SET datestyle TO 'ISO, MDY';

SELECT * from pxf_hdfs_small_data ORDER BY n1;
