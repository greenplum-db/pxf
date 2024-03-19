-- @description query01 for JDBC writable query
SET timezone='utc';

INSERT INTO pxf_jdbc_writable_date_wide_range_on SELECT * FROM gpdb_types_with_date_wide_range;

SELECT * FROM gpdb_types_target_with_date_wide_range ORDER BY t1;
