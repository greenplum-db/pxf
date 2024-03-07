-- @description query01 for JDBC query with date by partitioning
SET timezone='utc';

SELECT * FROM pxf_jdbc_multiple_fragments_by_date ORDER BY t1;
