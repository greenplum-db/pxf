-- @description query01 for JDBC writable query
INSERT INTO pxf_jdbc_writable SELECT * FROM gpdb_types;

SELECT * FROM gpdb_types_target ORDER BY t1;
