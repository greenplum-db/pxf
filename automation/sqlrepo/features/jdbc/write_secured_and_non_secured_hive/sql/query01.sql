-- @description query01 for Multiple JDBC Hive Server writing
SELECT * FROM pxf_jdbc_hive_readable ORDER BY t1;

SELECT * FROM pxf_jdbc_hive_non_secure_readable ORDER BY t1;

INSERT INTO pxf_jdbc_hive_writable SELECT * FROM jdbc_write_hive_supported_types;

INSERT INTO pxf_jdbc_hive_non_secure_writable SELECT * FROM jdbc_write_hive_supported_types;

SELECT * FROM jdbc_write_hive_supported_types ORDER BY t1;

SELECT * FROM pxf_jdbc_hive_readable ORDER BY t1;

SELECT * FROM pxf_jdbc_hive_non_secure_readable ORDER BY t1;