-- @description query01 for JDBC writable query
INSERT INTO pxf_jdbc_writable_pool SELECT t1, t2, num1 FROM gpdb_types;

SELECT * FROM gpdb_types_pool_target ORDER BY t1;
