-- @description query01 for primitive Parquet data types
-- Parquet data has been generated using PDT timezone, so we need to shift tm field on difference between PDT and current timezone
-- TODO: generate Parquet data on fly
SELECT t1, t2, num1, dub1, dec1, tm, r, bg, b, tn, sml, vc1, c1, bin FROM parquet_view ORDER BY t1;
