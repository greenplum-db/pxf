-- @description query01 for writing undefined precision numeric with NULL flag on. When try to write a numeric with data size > HiveDecimal.MAX_PRECISION, the numeric will be set to NULL
SELECT * FROM pxf_parquet_read_undefined_precision_numeric_null_flag ORDER BY description;
