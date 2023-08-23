-- @description query01 for JDBC query with batch size 0 (as good as infinity)
SELECT * FROM pxf_jdbc_readable_nobatch ORDER BY t1;
