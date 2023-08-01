-- @description query01 for JDBC query with int by partitioning
SET datestyle TO 'ISO, MDY';
SELECT t2, tm, sqrt(sml), c1 FROM pxf_jdbc_multiple_fragments_by_int ORDER BY t1;
