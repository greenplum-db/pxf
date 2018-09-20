-- @description query01 for PXF Hive filter pushdown with hex delimiter
SET gp_external_enable_filter_pushdown = true;

SET optimizer = off;

SELECT * FROM test_filter WHERE t0 = 'J' and a1 = 9 ORDER BY t0, a1;

SET optimizer = on;

SELECT * FROM test_filter WHERE t0 = 'J' and a1 = 9 ORDER BY t0, a1;
