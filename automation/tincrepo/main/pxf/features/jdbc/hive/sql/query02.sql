-- @description query02 for JDBC Hive query without partitioning
SELECT s1, n1 FROM pxf_jdbc_hive_types_table WHERE tn < 11 ORDER BY n1;
