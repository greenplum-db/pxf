-- @description query01 for JDBC Hive query without partitioning
SET datestyle TO 'ISO, MDY';
SELECT s1, s2, n1, d1, round(dc1, 18), tm, f, bg, b, tn, sml, dt, vc1, c1 FROM pxf_jdbc_hive_types_table ORDER BY s1;
