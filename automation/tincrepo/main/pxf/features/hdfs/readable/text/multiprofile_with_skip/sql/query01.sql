-- @description query01 for PXF HDFS Readable Text Data for :multi:profile
SET datestyle TO 'ISO, MDY';

SELECT * from pxf_hdfs_data_multi_profile_with_skip ORDER BY n1;
