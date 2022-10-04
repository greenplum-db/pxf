-- @description query01 for PXF HDFS Writable Avro without user-provided schema, primitive types

-- start_matchsubs
--
-- # create a match/subs
--
-- end_matchsubs
SET extra_float_digits=0;

SELECT * from writable_avro_primitive_generate_schema_readable ORDER BY type_int;
