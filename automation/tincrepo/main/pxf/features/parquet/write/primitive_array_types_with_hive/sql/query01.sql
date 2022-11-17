-- @description query01 for writing parquet primitive array data types using the Hive profile
SET bytea_output=hex;

-- for the numeric column in GPDB, decimal(38,18) in Hive, there are differences in output between different hive versions.
-- make the print consistent by doing `round(numeric_arr, 18) as numeric_arr` to force the same precision.
-- SELECT id, bool_arr, bigint_arr, small_arr, int_arr, text_arr, real_arr, float_arr, char_arr, varchar_arr, varchar_arr_nolimit, date_arr, timestamp_arr, numeric_arr, FROM pxf_parquet_write_array_with_hive_readable ORDER BY id;
SELECT * FROM pxf_parquet_write_array_with_hive_readable ORDER BY id;

