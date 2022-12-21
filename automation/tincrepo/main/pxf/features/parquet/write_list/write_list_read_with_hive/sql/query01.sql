-- @description query01 for writing Parquet List data then reading with Hive (except Timestamp List and Binary List)
-- Hive doesn't support Parquet Timestamp List. And Hive JDBC cannot handle bytea array correctly.
SELECT * FROM pxf_parquet_write_list_read_with_hive_readable ORDER BY id