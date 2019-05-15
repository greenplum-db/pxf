-- @description query02 for JDBC Hive query without partitioning
SELECT s1, n1 FROM pxf_jdbc_hive_small_data WHERE d1 > 50 ORDER BY n1;