-- @description query01 for writing defined precision numeric with ERROR flag on. When try to write a numeric with precision > HiveDecimal.MAX_PRECISION, an error will be thrown.
INSERT INTO parquet_write_defined_precision_numeric_error_flag SELECT * FROM numeric_precision;
