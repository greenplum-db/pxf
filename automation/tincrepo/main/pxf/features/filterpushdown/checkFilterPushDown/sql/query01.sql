-- @description query01 for PXF Hive filter pushdown case
--
-- start_matchsubs
--
-- # filter values that are equivalent but have different operand order
--
-- m/a1c23s1d1o5a1c23s2d10o5l1a0c25s1dBo5l0/
-- s/a1c23s1d1o5a1c23s2d10o5l1a0c25s1dBo5l0/a0c25s1dBo5a1c23s1d1o5a1c23s2d10o5l1l0/
--
-- m/a1c23s1d5o1a2c16s4dtrueo0l2l0/
-- s/a1c23s1d5o1a2c16s4dtrueo0l2l0/a2c16s4dtrueo0l2a1c23s1d5o1l0/
--
-- m/a1c23s1d1o3a0c25s1dBo5l0/
-- s/a1c23s1d1o3a0c25s1dBo5l0/a0c25s1dBo5a1c23s1d1o3l0/
--
-- m/a1c23s1d1o5a1c23s2d10o5l1a0c25s1dBo5l0/
-- s/a1c23s1d1o5a1c23s2d10o5l1a0c25s1dBo5l0/a0c25s1dBo5a1c23s1d1o5a1c23s2d10o5l1l0/
--
-- end_matchsubs

SET gp_external_enable_filter_pushdown = true;

SET optimizer = off;

SELECT * FROM test_filter WHERE  t0 = 'A' and a1 = 0 ORDER BY t0, a1;

SELECT * FROM test_filter WHERE  t0 = 'B' AND a1 <= 1 ORDER BY t0, a1;

SELECT * FROM test_filter WHERE  t0 = 'B' AND (a1 = 1 OR a1 = 10) ORDER BY t0, a1;

SELECT * FROM test_filter WHERE  t0 = 'B' OR (a1 >= 0 AND a1 <= 2) ORDER BY t0, a1;

SELECT * FROM test_filter WHERE  b2 = false ORDER BY t0, a1;

SELECT t0, a1, filtervalue FROM test_filter WHERE a1 < 5 AND b2 = false ORDER BY t0, a1;

SELECT round(sqrt(a1)::numeric,5), filtervalue FROM test_filter WHERE a1 < 5 AND b2 = false ORDER BY t0, a1;

SELECT round(sqrt(a1)::numeric,5), filtervalue FROM test_filter WHERE b2 = false ORDER BY t0;

SELECT * FROM test_filter WHERE  b2 = false AND (a1 = 1 OR a1 = 10) ORDER BY t0, a1;

SELECT * FROM test_filter WHERE  b2 = false OR (a1 >= 0 AND a1 <= 2) ORDER BY t0, a1;

SET optimizer = on;

SELECT * FROM test_filter WHERE  t0 = 'A' and a1 = 0 ORDER BY t0, a1;

SELECT * FROM test_filter WHERE  t0 = 'B' AND a1 <= 1 ORDER BY t0, a1;

SELECT * FROM test_filter WHERE  t0 = 'B' AND (a1 = 1 OR a1 = 10) ORDER BY t0, a1;

SELECT * FROM test_filter WHERE  t0 = 'B' OR (a1 >= 0 AND a1 <= 2) ORDER BY t0, a1;

SELECT * FROM test_filter WHERE  b2 = false ORDER BY t0, a1;

SELECT t0, a1, filtervalue FROM test_filter WHERE a1 < 5 AND b2 = false ORDER BY t0, a1;

SELECT round(sqrt(a1)::numeric,5), filtervalue FROM test_filter WHERE a1 < 5 AND b2 = false ORDER BY t0, a1;

SELECT round(sqrt(a1)::numeric,5), filtervalue FROM test_filter WHERE b2 = false ORDER BY t0;

SELECT * FROM test_filter WHERE  b2 = false AND (a1 = 1 OR a1 = 10) ORDER BY t0, a1;

SELECT * FROM test_filter WHERE  b2 = false OR (a1 >= 0 AND a1 <= 2) ORDER BY t0, a1;
