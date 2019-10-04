-- @description query01 for Multiple JDBC Hive Server queries without partitioning
SELECT s1, s2, n1, d1, round(dc1, 18), tm, f, bg, b, tn, sml, dt, vc1, c1 FROM pxf_jdbc_hive_types_table ORDER BY s1;

SELECT s1, s2, n1, d1, round(dc1, 18), tm, f, bg, b, tn, sml, dt, vc1, c1 FROM pxf_jdbc_hive_2_types_table ORDER BY s1;

SELECT s1, s2, n1, d1, round(dc1, 18), tm, f, bg, b, tn, sml, dt, vc1, c1 FROM pxf_jdbc_hive_types_table UNION ALL
SELECT s1, s2, n1, d1, round(dc1, 18), tm, f, bg, b, tn, sml, dt, vc1, c1 FROM pxf_jdbc_hive_2_types_table
ORDER BY s1;
