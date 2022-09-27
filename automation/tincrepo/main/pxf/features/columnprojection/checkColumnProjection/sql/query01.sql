-- start_ignore
DROP TABLE IF EXISTS t0_values;
CREATE TABLE t0_values(key char(1), value int) DISTRIBUTED BY (key);
INSERT INTO t0_values VALUES('A', 50);
-- end_ignore
-- @description query01 for PXF Column Projection Support

SET optimizer = off;

SELECT * FROM test_column_projection ORDER BY t0;

SELECT t0, colprojvalue FROM test_column_projection ORDER BY t0;

SELECT colprojvalue FROM test_column_projection ORDER BY t0;

SELECT t0, colprojvalue FROM test_column_projection WHERE b2 ORDER BY t0;

SELECT t0, a1, colprojvalue FROM test_column_projection WHERE a1 < 5 AND b2 = false ORDER BY t0;

SELECT sqrt(a1), colprojvalue FROM test_column_projection WHERE a1 < 5 AND b2 = false ORDER BY t0;

SELECT sqrt(a1), colprojvalue FROM test_column_projection WHERE b2 = false ORDER BY t0;

SELECT t0, colprojvalue FROM test_column_projection WHERE a1 < 5 ORDER BY t0;

SELECT t0, colprojvalue FROM test_column_projection WHERE a1 <= 5 ORDER BY t0;

SELECT t0, colprojvalue FROM test_column_projection GROUP BY t0, colprojvalue HAVING AVG(a1) < 5 ORDER BY t0;

SELECT b.value, a.colprojvalue FROM test_column_projection a JOIN t0_values b ON a.t0 = b.key;

SELECT t0, colprojvalue FROM test_column_projection WHERE a1 < 2 OR a1 >= 8 ORDER BY t0;

SELECT t0, colprojvalue FROM test_column_projection WHERE sqrt(a1) > 1 ORDER BY t0;

SELECT t0, colprojvalue, sqrt(a1) FROM test_column_projection ORDER BY t0;

-- Casting boolean column to int
SELECT t0, colprojvalue, sqrt(b2::int) FROM test_column_projection ORDER BY t0;

SET optimizer = on;

SELECT * FROM test_column_projection ORDER BY t0;

SELECT t0, colprojvalue FROM test_column_projection ORDER BY t0;

SELECT colprojvalue FROM test_column_projection ORDER BY t0;

SELECT t0, colprojvalue FROM test_column_projection WHERE b2 ORDER BY t0;

SELECT t0, a1, colprojvalue FROM test_column_projection WHERE a1 < 5 AND b2 = false ORDER BY t0;

SELECT sqrt(a1), colprojvalue FROM test_column_projection WHERE a1 < 5 AND b2 = false ORDER BY t0;

SELECT sqrt(a1), colprojvalue FROM test_column_projection WHERE b2 = false ORDER BY t0;

SELECT t0, colprojvalue FROM test_column_projection WHERE a1 < 5 ORDER BY t0;

SELECT t0, colprojvalue FROM test_column_projection WHERE a1 <= 5 ORDER BY t0;

SELECT t0, colprojvalue FROM test_column_projection GROUP BY t0, colprojvalue HAVING AVG(a1) < 5 ORDER BY t0;

SELECT b.value, a.colprojvalue FROM test_column_projection a JOIN t0_values b ON a.t0 = b.key;

SELECT t0, colprojvalue FROM test_column_projection WHERE a1 < 2 OR a1 >= 8 ORDER BY t0;

SELECT t0, colprojvalue FROM test_column_projection WHERE sqrt(a1) > 1 ORDER BY t0;

SELECT t0, colprojvalue, sqrt(a1) FROM test_column_projection ORDER BY t0;

-- Casting boolean column to int
SELECT t0, colprojvalue, sqrt(b2::int) FROM test_column_projection ORDER BY t0;

-- cleanup
-- start_ignore
DROP TABLE IF EXISTS t0_values;
-- end_ignore
