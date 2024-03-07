-- @description query01 for JDBC writable query
SET timezone='America/Los_Angeles';

INSERT INTO pxf_jdbc_writable_date_wide_range_on SELECT * FROM gpdb_types;

SELECT * FROM gpdb_types_target ORDER BY t1;
