-- @description query01 for PXF Hive partitioned table cases

SELECT * FROM pxf_hive_partitioned_table ORDER BY fmt, t1;

SELECT * FROM pxf_hive_partitioned_table WHERE fmt = 'abcd' ORDER BY fmt, t1;

SELECT * FROM pxf_hive_partitioned_table WHERE fmt IS NULL ORDER BY fmt, t1;