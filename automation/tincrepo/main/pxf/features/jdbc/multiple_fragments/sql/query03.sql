-- @description query01 for JDBC query with enum by partitioning
SET datestyle TO 'ISO, MDY';
SELECT * FROM pxf_jdbc_multiple_fragments_by_enum ORDER BY t1;
