-- @description query01 for JDBC query without partitioning
SET timezone='utc';

SELECT * FROM pxf_jdbc_single_fragment ORDER BY t1;
