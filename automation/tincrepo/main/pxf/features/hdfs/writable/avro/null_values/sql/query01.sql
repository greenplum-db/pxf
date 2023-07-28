-- @description query01 for PXF HDFS Writable Avro without user-provided schema, complex types with null values

-- start_matchsubs
--
-- # create a match/subs
--
-- end_matchsubs
SELECT * from writable_avro_null_values_readable ORDER BY type_int;
