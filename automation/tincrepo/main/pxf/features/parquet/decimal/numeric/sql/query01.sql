-- @description query01 for writing defined precision numeric with pxf.parquet.write.decimal.overflow = ignore. When try to write a numeric with precision > HiveDecimal.MAX_PRECISION, the numeric will be set to NULL
SELECT a, b, c, d, e, f, g, h FROM pxf_parquet_read_numeric ORDER BY description;
