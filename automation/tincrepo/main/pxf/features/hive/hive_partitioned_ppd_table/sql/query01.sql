-- @description query01 for PXF Hive partitioned table with predicate pushdown tests cases for logical operators
--
-- Terminology
-- P stands for predicates conforming with hive partition filtering
-- NP stands for predicates not conforming with hive partition filtering
--
-- (P1 AND P2) OR (P3 AND P4)
SELECT * FROM pxf_hive_partitioned_ppd_table WHERE (n1=4 AND s2='s_9') OR (n1=1 AND s2='s_6') ORDER BY n1, s2;
-- (P1 AND P2) OR .... total of 10 times spanning all partitions
SELECT * FROM pxf_hive_partitioned_ppd_table WHERE (n1=1 AND s2='s_6') OR (n1=2 AND s2='s_7') OR (n1=3 AND s2='s_8') OR (n1=4 AND s2='s_9') OR (n1=5 AND s2='s_10') OR (n1=6 AND s2='s_11') OR (n1=7 AND s2='s_12') OR (n1=8 AND s2='s_13') OR (n1=9 AND s2='s_14') OR (n1=10 AND s2='s_15') ORDER BY n1, s2;
-- (P1 AND P2) UNION ALL (P3 AND P4)...
SELECT * FROM pxf_hive_partitioned_ppd_table WHERE (n1=1 AND s2='s_6') UNION ALL
SELECT * FROM pxf_hive_partitioned_ppd_table WHERE (n1=2 AND s2='s_7') UNION ALL
SELECT * FROM pxf_hive_partitioned_ppd_table WHERE (n1=3 AND s2='s_8') UNION ALL
SELECT * FROM pxf_hive_partitioned_ppd_table WHERE (n1=4 AND s2='s_9') UNION ALL
SELECT * FROM pxf_hive_partitioned_ppd_table WHERE (n1=5 AND s2='s_10') UNION ALL
SELECT * FROM pxf_hive_partitioned_ppd_table WHERE (n1=7 AND s2='s_12') UNION ALL
SELECT * FROM pxf_hive_partitioned_ppd_table WHERE (n1=6 AND s2='s_11') UNION ALL
SELECT * FROM pxf_hive_partitioned_ppd_table WHERE (n1=8 AND s2='s_13') UNION ALL
SELECT * FROM pxf_hive_partitioned_ppd_table WHERE (n1=9 AND s2='s_14') UNION ALL
SELECT * FROM pxf_hive_partitioned_ppd_table WHERE (n1=10 AND s2='s_15') ORDER BY n1, s2;

-- (P1 AND P2) AND NP
SELECT * FROM pxf_hive_partitioned_ppd_table WHERE (n1=4 AND s2='s_9') AND (s1='row1') ORDER BY n1, s2;
-- (P1 AND P2) OR NP
SELECT * FROM pxf_hive_partitioned_ppd_table WHERE (n1=4 AND s2='s_9') OR (s1='row1') ORDER BY n1, s2;
-- Test for NOT operator
SELECT * FROM pxf_hive_partitioned_ppd_table WHERE (d1 >=9) AND NOT(n1=4 AND s2='s_9') ORDER BY n1,s2;
-- Test for !=
SELECT * FROM pxf_hive_partitioned_ppd_table WHERE (n1!=4 AND s2!='s_9') ORDER BY n1, s2;

-- Test for max filters hitting all but one partition
SELECT * FROM pxf_hive_partitioned_ppd_table WHERE (n1=2 AND s2='s_7') OR (n1=3 AND s2='s_8') OR (n1=4 AND s2='s_9') OR (n1=5 AND s2='s_10') OR (n1=6 AND s2='s_11') OR (n1=7 AND s2='s_12') OR (n1=8 AND s2='s_13') OR (n1=9 AND s2='s_14') OR (n1=10 AND s2='s_15') OR
(n1=2 AND s2='s_7') OR (n1=3 AND s2='s_8') OR (n1=4 AND s2='s_9') OR (n1=5 AND s2='s_10') OR (n1=6 AND s2='s_11') OR (n1=7 AND s2='s_12') OR (n1=8 AND s2='s_13') OR (n1=9 AND s2='s_14') OR (n1=10 AND s2='s_15') OR
(n1=2 AND s2='s_7') OR (n1=3 AND s2='s_8') OR (n1=4 AND s2='s_9') OR (n1=5 AND s2='s_10') OR (n1=6 AND s2='s_11') OR (n1=7 AND s2='s_12') OR (n1=8 AND s2='s_13') OR (n1=9 AND s2='s_14') OR (n1=10 AND s2='s_15') OR
(n1=2 AND s2='s_7') OR (n1=3 AND s2='s_8') OR (n1=4 AND s2='s_9') OR (n1=5 AND s2='s_10') OR (n1=6 AND s2='s_11') OR (n1=7 AND s2='s_12') OR (n1=8 AND s2='s_13') OR (n1=9 AND s2='s_14') OR (n1=10 AND s2='s_15') OR
(n1=2 AND s2='s_7') OR (n1=3 AND s2='s_8') OR (n1=4 AND s2='s_9') OR (n1=5 AND s2='s_10') OR (n1=6 AND s2='s_11') OR (n1=7 AND s2='s_12') OR (n1=8 AND s2='s_13') OR (n1=9 AND s2='s_14') OR (n1=10 AND s2='s_15') OR
(n1=2 AND s2='s_7') OR (n1=3 AND s2='s_8') OR (n1=4 AND s2='s_9') OR (n1=5 AND s2='s_10') OR (n1=6 AND s2='s_11') OR (n1=7 AND s2='s_12') OR (n1=8 AND s2='s_13') OR (n1=9 AND s2='s_14') OR (n1=10 AND s2='s_15') OR
(n1=2 AND s2='s_7') OR (n1=3 AND s2='s_8') OR (n1=4 AND s2='s_9') OR (n1=5 AND s2='s_10') OR (n1=6 AND s2='s_11') OR (n1=7 AND s2='s_12') OR (n1=8 AND s2='s_13') OR (n1=9 AND s2='s_14') OR (n1=10 AND s2='s_15') OR
(n1=2 AND s2='s_7') OR (n1=3 AND s2='s_8') OR (n1=4 AND s2='s_9') OR (n1=5 AND s2='s_10') OR (n1=6 AND s2='s_11') OR (n1=7 AND s2='s_12') OR (n1=8 AND s2='s_13') OR (n1=9 AND s2='s_14') OR (n1=10 AND s2='s_15') OR
(n1=2 AND s2='s_7') OR (n1=3 AND s2='s_8') OR (n1=4 AND s2='s_9') OR (n1=5 AND s2='s_10') OR (n1=6 AND s2='s_11') OR (n1=7 AND s2='s_12') OR (n1=8 AND s2='s_13') OR (n1=9 AND s2='s_14') OR (n1=10 AND s2='s_15') OR
(n1=2 AND s2='s_7') OR (n1=3 AND s2='s_8') OR (n1=4 AND s2='s_9') OR (n1=5 AND s2='s_10') OR (n1=6 AND s2='s_11') OR (n1=7 AND s2='s_12') OR (n1=8 AND s2='s_13') OR (n1=9 AND s2='s_14') OR (n1=10 AND s2='s_15') OR
(n1=2 AND s2='s_7') OR (n1=3 AND s2='s_8') OR (n1=4 AND s2='s_9') OR (n1=5 AND s2='s_10') OR (n1=6 AND s2='s_11') OR (n1=7 AND s2='s_12') OR (n1=8 AND s2='s_13') OR (n1=9 AND s2='s_14') OR (n1=10 AND s2='s_15') ORDER BY n1, s2;
