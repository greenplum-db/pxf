-- @description query01 for PXF HDFS Readable Text CSV Files with header
SET datestyle TO 'ISO, MDY';

SELECT * from pxf_hcfs_csv_files_with_header ORDER BY l_orderkey, l_partkey;
