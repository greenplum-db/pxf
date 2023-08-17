-- @description query01 for PXF filter pushdown case
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
-- m/a2c16s4dtrueo0l2a1c23s1d5o1l0/
-- s/a2c16s4dtrueo0l2a1c23s1d5o1l0/a1c23s1d5o1a2c16s4dtrueo0l2l0
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

-- test text predicates
SELECT * FROM test_filter WHERE  t0 =  'B' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  t0 =  'B ' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  t0 <  'B' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  t0 <= 'B' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  t0 >  'B' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  t0 >= 'B' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  t0 <> 'B' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  t0 LIKE     'B%' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  t0 NOT LIKE 'B%' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  t0 IN     ('B','C') ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  t0 NOT IN ('B','C') ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  t0 BETWEEN     'A' AND 'C' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  t0 NOT BETWEEN 'A' AND 'C' ORDER BY t0, a1;

-- test integer predicates
SELECT * FROM test_filter WHERE  a1 =  1 ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  a1 <  1 ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  a1 <= 1 ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  a1 >  1 ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  a1 >= 1 ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  a1 <> 1 ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  a1 IN     (1,2) ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  a1 NOT IN (1,2) ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  a1 BETWEEN     1 AND 3 ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  a1 NOT BETWEEN 1 AND 3 ORDER BY t0, a1;

-- test numeric predicates
SELECT * FROM test_filter WHERE  c3 =  1.11 ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  c3 <  1.11 ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  c3 <= 1.11 ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  c3 >  1.11 ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  c3 >= 1.11 ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  c3 <> 1.11 ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  c3 IN     (1.11,2.21) ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  c3 NOT IN (1.11,2.21) ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  c3 BETWEEN     1.11 AND 3.31 ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  c3 NOT BETWEEN 1.11 AND 3.31 ORDER BY t0, a1;

-- test char predicates
SELECT * FROM test_filter WHERE  d4 =  'BB' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  d4 =  'BB ' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  d4 <  'BB' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  d4 <= 'BB' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  d4 >  'BB' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  d4 >= 'BB' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  d4 <> 'BB' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  d4 LIKE     'B%' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  d4 NOT LIKE 'B%' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  d4 IN     ('BB','CC') ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  d4 NOT IN ('BB','CC') ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  d4 BETWEEN     'AA' AND 'CC' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  d4 NOT BETWEEN 'AA' AND 'CC' ORDER BY t0, a1;

-- test varchar predicates
SELECT * FROM test_filter WHERE  e5 =  'BB' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  e5 =  'BB ' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  e5 <  'BB' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  e5 <= 'BB' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  e5 >  'BB' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  e5 >= 'BB' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  e5 <> 'BB' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  e5 LIKE     'B%' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  e5 NOT LIKE 'B%' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  e5 IN     ('BB','CC') ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  e5 NOT IN ('BB','CC') ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  e5 BETWEEN     'AA' AND 'CC' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  e5 NOT BETWEEN 'AA' AND 'CC' ORDER BY t0, a1;

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

-- test text predicates
SELECT * FROM test_filter WHERE  t0 =  'B' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  t0 =  'B ' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  t0 <  'B' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  t0 <= 'B' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  t0 >  'B' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  t0 >= 'B' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  t0 <> 'B' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  t0 LIKE     'B%' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  t0 NOT LIKE 'B%' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  t0 IN     ('B','C') ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  t0 NOT IN ('B','C') ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  t0 BETWEEN     'A' AND 'C' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  t0 NOT BETWEEN 'A' AND 'C' ORDER BY t0, a1;

-- test integer predicates
SELECT * FROM test_filter WHERE  a1 =  1 ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  a1 <  1 ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  a1 <= 1 ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  a1 >  1 ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  a1 >= 1 ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  a1 <> 1 ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  a1 IN     (1,2) ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  a1 NOT IN (1,2) ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  a1 BETWEEN     1 AND 3 ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  a1 NOT BETWEEN 1 AND 3 ORDER BY t0, a1;

-- test numeric predicates
SELECT * FROM test_filter WHERE  c3 =  1.11 ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  c3 <  1.11 ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  c3 <= 1.11 ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  c3 >  1.11 ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  c3 >= 1.11 ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  c3 <> 1.11 ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  c3 IN     (1.11,2.21) ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  c3 NOT IN (1.11,2.21) ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  c3 BETWEEN     1.11 AND 3.31 ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  c3 NOT BETWEEN 1.11 AND 3.31 ORDER BY t0, a1;

-- test char predicates
SELECT * FROM test_filter WHERE  d4 =  'BB' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  d4 =  'BB ' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  d4 <  'BB' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  d4 <= 'BB' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  d4 >  'BB' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  d4 >= 'BB' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  d4 <> 'BB' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  d4 LIKE     'B%' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  d4 NOT LIKE 'B%' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  d4 IN     ('BB','CC') ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  d4 NOT IN ('BB','CC') ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  d4 BETWEEN     'AA' AND 'CC' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  d4 NOT BETWEEN 'AA' AND 'CC' ORDER BY t0, a1;

-- test varchar predicates
SELECT * FROM test_filter WHERE  e5 =  'BB' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  e5 =  'BB ' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  e5 <  'BB' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  e5 <= 'BB' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  e5 >  'BB' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  e5 >= 'BB' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  e5 <> 'BB' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  e5 LIKE     'B%' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  e5 NOT LIKE 'B%' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  e5 IN     ('BB','CC') ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  e5 NOT IN ('BB','CC') ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  e5 BETWEEN     'AA' AND 'CC' ORDER BY t0, a1;
SELECT * FROM test_filter WHERE  e5 NOT BETWEEN 'AA' AND 'CC' ORDER BY t0, a1;

