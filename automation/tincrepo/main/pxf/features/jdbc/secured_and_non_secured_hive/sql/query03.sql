-- @description query03 for Multiple JDBC Hive Server queries with timestamp filter
SELECT s1, n1, tm FROM pxf_jdbc_hive_types_table WHERE tm > '2013-07-23 21:00:01' ORDER BY n1;

SELECT s1, n1, tm FROM pxf_jdbc_hive_non_secure_types_table WHERE tm > '2013-07-23 21:00:01' ORDER BY n1;

SELECT s1, n1, tm FROM pxf_jdbc_hive_types_table WHERE tm > '2013-07-23 21:00:01' UNION ALL
SELECT s1, n1, tm FROM pxf_jdbc_hive_non_secure_types_table WHERE tm > '2013-07-23 21:00:01'
ORDER BY n1;
