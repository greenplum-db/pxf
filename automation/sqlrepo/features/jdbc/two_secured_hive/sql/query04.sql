-- @description query04 for Multiple JDBC Hive Server queries with date filter
SELECT s1, n1, dt FROM pxf_jdbc_hive_types_table WHERE dt = '2015-03-06' ORDER BY n1, s1;

SELECT s1, n1, dt FROM pxf_jdbc_hive_2_types_table WHERE dt = '2015-03-06' ORDER BY n1, s1;

SELECT s1, n1, dt FROM pxf_jdbc_hive_types_table WHERE dt = '2015-03-06' UNION ALL
SELECT s1, n1, dt FROM pxf_jdbc_hive_2_types_table WHERE dt = '2015-03-06'
ORDER BY n1, s1;
