-- @description query04 for JDBC Hive query with date filter
SET datestyle TO 'ISO, MDY';
SELECT s1, n1, dt FROM pxf_jdbc_hive_types_table WHERE dt = '2015-03-06' ORDER BY n1, s1;
