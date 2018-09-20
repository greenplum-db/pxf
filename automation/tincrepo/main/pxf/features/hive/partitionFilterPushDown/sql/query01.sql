-- @description query01 for PXF Hive filter pushdown case

SELECT * FROM hive_partition_filter_pushdown_rc ORDER BY fmt, t1;

SELECT * FROM hive_partition_filter_pushdown_seq ORDER BY fmt, t1;

SELECT * FROM hive_partition_filter_pushdown_txt ORDER BY fmt, t1;

SELECT * FROM hive_partition_filter_pushdown_orc ORDER BY fmt, t1;

SELECT * FROM hive_partition_filter_pushdown_none ORDER BY fmt, t1;

SELECT * FROM hive_partition_filter_pushdown_complex ORDER BY fmt, t1;