-- @description query01 for JDBC query with batch size 0 (as good as infinity)
SET timezone='America/Los_Angeles';

SELECT * FROM pxf_jdbc_readable_nobatch ORDER BY t1;
