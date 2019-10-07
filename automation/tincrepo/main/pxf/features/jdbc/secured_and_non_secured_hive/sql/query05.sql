-- @description query05 for Multiple JDBC Hive Server queries without partitioning
SELECT * FROM pxf_jdbc_hive_types_server_table ORDER BY n1;

SELECT * FROM pxf_jdbc_hive_non_secure_types_server_table ORDER BY n1;

SELECT * FROM pxf_jdbc_hive_types_server_table UNION ALL
SELECT * FROM pxf_jdbc_hive_non_secure_types_server_table
ORDER BY n1;
