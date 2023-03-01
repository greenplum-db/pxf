-- @description query01 for writing undefined precision numeric with pxf.parquet.write.decimal.overflow = error. When try to write a numeric with data size > HiveDecimal.MAX_PRECISION, an error will be thrown.
INSERT INTO pxf_parquet_write_undefined_precision_numeric_large_data_length SELECT * FROM numeric_undefined_precision;
