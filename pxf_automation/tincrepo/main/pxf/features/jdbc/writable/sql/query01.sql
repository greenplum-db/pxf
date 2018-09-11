-- @description query01 for JDBC writable query
INSERT INTO pxf_jdbc_writable SELECT * FROM hawq_types;

SELECT * FROM hawq_types_target ORDER BY t1;
