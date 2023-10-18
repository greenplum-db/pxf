-- @description query01 for writing defined precision numeric with data precision <= HiveDecimal.MAX_PRECISION
SELECT a, b, c, d, e, f, g, h FROM pxf_parquet_read_numeric ORDER BY description;