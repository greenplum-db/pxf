-- @description query01 for PXF HDFS Writable Avro with user-provided schema on Classpath, complex types with arrays containing nulls

-- start_matchsubs
--
-- # create a match/subs
--
-- end_matchsubs
SELECT * from writable_avro_array_user_schema_w_nulls_readable ORDER BY type_int;
