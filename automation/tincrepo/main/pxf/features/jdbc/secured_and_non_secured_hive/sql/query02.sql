-- @description query02 for Multiple JDBC Hive Server queries without partitioning
SELECT s1, n1 FROM pxf_jdbc_hive_types_table WHERE tn < 11 ORDER BY n1;

SELECT s1, n1 FROM pxf_jdbc_hive_non_secure_types_table WHERE tn < 11 ORDER BY n1;

SELECT s1, n1 FROM pxf_jdbc_hive_types_table WHERE tn < 11 UNION ALL
SELECT s1, n1 FROM pxf_jdbc_hive_non_secure_types_table WHERE tn < 11
ORDER BY n1, s1;
