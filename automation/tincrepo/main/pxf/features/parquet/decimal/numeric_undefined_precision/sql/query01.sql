-- @description query01 for writing undefined precision numeric with data precision <= HiveDecimal.MAX_PRECISION
SELECT * FROM pxf_parquet_read_undefined_precision_numeric ORDER BY description;
