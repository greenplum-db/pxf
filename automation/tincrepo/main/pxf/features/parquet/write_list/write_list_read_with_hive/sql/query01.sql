-- @description query01 for writing Parquet List data then reading using the Hive profile
SET bytea_output=hex;

SELECT * FROM pxf_parquet_write_list_read_with_hive_readable ORDER BY id;
