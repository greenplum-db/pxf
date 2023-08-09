-- @description query01 for primitive Parquet data types
-- Parquet data has been generated using PDT timezone, so we need to shift tm field on difference between PDT and current timezone
-- TODO: generate Parquet data on fly
SET datestyle TO 'ISO, MDY';
SELECT s1, s2, n1, d1, dc1, tm, f, bg, b, tn, sml, vc1, c1, bin FROM parquet_view ORDER BY s1;
