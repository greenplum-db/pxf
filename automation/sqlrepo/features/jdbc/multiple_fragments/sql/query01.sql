-- @description query01 for JDBC query with int by partitioning
SET timezone='utc';

SELECT * FROM pxf_jdbc_multiple_fragments_by_int ORDER BY t1;
