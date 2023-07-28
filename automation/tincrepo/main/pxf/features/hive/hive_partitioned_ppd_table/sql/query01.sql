-- @description query01 for PXF Hive partitioned table with predicate pushdown tests cases for logical operators
--
-- Terminology
-- P stands for predicates conforming with hive partition filtering
-- NP stands for predicates not conforming with hive partition filtering
--
-- (P1 AND P2) OR (P3 AND P4)
SELECT * FROM pxf_hive_partitioned_ppd_table WHERE (num1=4 AND t2='s_9') OR (num1=1 AND t2='s_6') ORDER BY num1, t2;
-- (P1 AND P2) OR .... total of 10 times spanning all partitions
SELECT * FROM pxf_hive_partitioned_ppd_table WHERE (num1=1 AND t2='s_6') OR (num1=2 AND t2='s_7') OR (num1=3 AND t2='s_8') OR (num1=4 AND t2='s_9') OR (num1=5 AND t2='s_10') OR (num1=6 AND t2='s_11') OR (num1=7 AND t2='s_12') OR (num1=8 AND t2='s_13') OR (num1=9 AND t2='s_14') OR (num1=10 AND t2='s_15') ORDER BY num1, t2;
-- (P1 AND P2) UNION ALL (P3 AND P4)...
SELECT * FROM pxf_hive_partitioned_ppd_table WHERE (num1=1 AND t2='s_6') UNION ALL
SELECT * FROM pxf_hive_partitioned_ppd_table WHERE (num1=2 AND t2='s_7') UNION ALL
SELECT * FROM pxf_hive_partitioned_ppd_table WHERE (num1=3 AND t2='s_8') UNION ALL
SELECT * FROM pxf_hive_partitioned_ppd_table WHERE (num1=4 AND t2='s_9') UNION ALL
SELECT * FROM pxf_hive_partitioned_ppd_table WHERE (num1=5 AND t2='s_10') UNION ALL
SELECT * FROM pxf_hive_partitioned_ppd_table WHERE (num1=7 AND t2='s_12') UNION ALL
SELECT * FROM pxf_hive_partitioned_ppd_table WHERE (num1=6 AND t2='s_11') UNION ALL
SELECT * FROM pxf_hive_partitioned_ppd_table WHERE (num1=8 AND t2='s_13') UNION ALL
SELECT * FROM pxf_hive_partitioned_ppd_table WHERE (num1=9 AND t2='s_14') UNION ALL
SELECT * FROM pxf_hive_partitioned_ppd_table WHERE (num1=10 AND t2='s_15') ORDER BY num1, t2;

-- (P1 AND P2) AND NP
SELECT * FROM pxf_hive_partitioned_ppd_table WHERE (num1=4 AND t2='s_9') AND (t1='row1') ORDER BY num1, t2;
-- (P1 AND P2) OR NP
SELECT * FROM pxf_hive_partitioned_ppd_table WHERE (num1=4 AND t2='s_9') OR (t1='row1') ORDER BY num1, t2;
-- Test for NOT operator
SELECT * FROM pxf_hive_partitioned_ppd_table WHERE (dub1 >=9) AND NOT(num1=4 AND t2='s_9') ORDER BY num1,t2;
-- Test for !=
SELECT * FROM pxf_hive_partitioned_ppd_table WHERE (num1!=4 AND t2!='s_9') ORDER BY num1, t2;

-- Test for max filters hitting all but one partition
SELECT * FROM pxf_hive_partitioned_ppd_table WHERE (num1=2 AND t2='s_7') OR (num1=3 AND t2='s_8') OR (num1=4 AND t2='s_9') OR (num1=5 AND t2='s_10') OR (num1=6 AND t2='s_11') OR (num1=7 AND t2='s_12') OR (num1=8 AND t2='s_13') OR (num1=9 AND t2='s_14') OR (num1=10 AND t2='s_15') OR
(num1=2 AND t2='s_7') OR (num1=3 AND t2='s_8') OR (num1=4 AND t2='s_9') OR (num1=5 AND t2='s_10') OR (num1=6 AND t2='s_11') OR (num1=7 AND t2='s_12') OR (num1=8 AND t2='s_13') OR (num1=9 AND t2='s_14') OR (num1=10 AND t2='s_15') OR
(num1=2 AND t2='s_7') OR (num1=3 AND t2='s_8') OR (num1=4 AND t2='s_9') OR (num1=5 AND t2='s_10') OR (num1=6 AND t2='s_11') OR (num1=7 AND t2='s_12') OR (num1=8 AND t2='s_13') OR (num1=9 AND t2='s_14') OR (num1=10 AND t2='s_15') OR
(num1=2 AND t2='s_7') OR (num1=3 AND t2='s_8') OR (num1=4 AND t2='s_9') OR (num1=5 AND t2='s_10') OR (num1=6 AND t2='s_11') OR (num1=7 AND t2='s_12') OR (num1=8 AND t2='s_13') OR (num1=9 AND t2='s_14') OR (num1=10 AND t2='s_15') OR
(num1=2 AND t2='s_7') OR (num1=3 AND t2='s_8') OR (num1=4 AND t2='s_9') OR (num1=5 AND t2='s_10') OR (num1=6 AND t2='s_11') OR (num1=7 AND t2='s_12') OR (num1=8 AND t2='s_13') OR (num1=9 AND t2='s_14') OR (num1=10 AND t2='s_15') OR
(num1=2 AND t2='s_7') OR (num1=3 AND t2='s_8') OR (num1=4 AND t2='s_9') OR (num1=5 AND t2='s_10') OR (num1=6 AND t2='s_11') OR (num1=7 AND t2='s_12') OR (num1=8 AND t2='s_13') OR (num1=9 AND t2='s_14') OR (num1=10 AND t2='s_15') OR
(num1=2 AND t2='s_7') OR (num1=3 AND t2='s_8') OR (num1=4 AND t2='s_9') OR (num1=5 AND t2='s_10') OR (num1=6 AND t2='s_11') OR (num1=7 AND t2='s_12') OR (num1=8 AND t2='s_13') OR (num1=9 AND t2='s_14') OR (num1=10 AND t2='s_15') OR
(num1=2 AND t2='s_7') OR (num1=3 AND t2='s_8') OR (num1=4 AND t2='s_9') OR (num1=5 AND t2='s_10') OR (num1=6 AND t2='s_11') OR (num1=7 AND t2='s_12') OR (num1=8 AND t2='s_13') OR (num1=9 AND t2='s_14') OR (num1=10 AND t2='s_15') OR
(num1=2 AND t2='s_7') OR (num1=3 AND t2='s_8') OR (num1=4 AND t2='s_9') OR (num1=5 AND t2='s_10') OR (num1=6 AND t2='s_11') OR (num1=7 AND t2='s_12') OR (num1=8 AND t2='s_13') OR (num1=9 AND t2='s_14') OR (num1=10 AND t2='s_15') OR
(num1=2 AND t2='s_7') OR (num1=3 AND t2='s_8') OR (num1=4 AND t2='s_9') OR (num1=5 AND t2='s_10') OR (num1=6 AND t2='s_11') OR (num1=7 AND t2='s_12') OR (num1=8 AND t2='s_13') OR (num1=9 AND t2='s_14') OR (num1=10 AND t2='s_15') OR
(num1=2 AND t2='s_7') OR (num1=3 AND t2='s_8') OR (num1=4 AND t2='s_9') OR (num1=5 AND t2='s_10') OR (num1=6 AND t2='s_11') OR (num1=7 AND t2='s_12') OR (num1=8 AND t2='s_13') OR (num1=9 AND t2='s_14') OR (num1=10 AND t2='s_15') ORDER BY num1, t2;
