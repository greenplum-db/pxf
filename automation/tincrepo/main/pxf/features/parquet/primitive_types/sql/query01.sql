-- @description query01 for primitive Parquet data types
-- Parquet data has been generated using PDT timezone, so we need to shift tm field on difference between PDT and current timezone
-- TODO: generate Parquet data on fly
SELECT t1, t2, num1, dub1, dec1, CAST (((CAST(tm AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'PDT') AT TIME ZONE current_setting('TIMEZONE')) AS TIMESTAMP WITHOUT TIME ZONE) as tm, r, bg, b, tn, sml, vc1, c1, bin FROM pxf_parquet_primitive_types ORDER BY t1;