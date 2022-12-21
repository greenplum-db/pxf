-- @description query01 for writing Parquet List data then reading with Hive (except Timestamp List and Binary List)
SET bytea_output=hex;

SELECT id,bool_arr,smallint_arr,int_arr,bigint_arr,real_arr,double_arr,text_arr,char_arr,varchar_arr,date_arr,numeric_arr
FROM pxf_parquet_write_list_read_with_hive_readable ORDER BY id