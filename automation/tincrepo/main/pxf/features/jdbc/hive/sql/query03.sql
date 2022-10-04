-- @description query03 for JDBC Hive query with timestamp filter
SELECT s1, n1, tm FROM pxf_jdbc_hive_types_table WHERE tm > '2013-07-23 21:00:01' ORDER BY n1, s1;
