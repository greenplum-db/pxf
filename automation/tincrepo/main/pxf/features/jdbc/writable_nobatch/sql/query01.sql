-- @description query01 for JDBC writable query
INSERT INTO pxf_jdbc_writable_nobatch SELECT t1, t2, num1 FROM gpdb_types;

SELECT * FROM gpdb_types_nobatch_target ORDER BY t1;
