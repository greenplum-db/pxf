-- @description query01 for writing Parquet List data with an invalid schema file provided

INSERT INTO parquet_list_user_provided_invalid_schema_write
SELECT id, bool_arr, smallint_arr, int_arr, bigint_arr, real_arr, double_arr, text_arr, bytea_arr, char_arr, varchar_arr, numeric_arr, date_arr
FROM pxf_parquet_list_types;