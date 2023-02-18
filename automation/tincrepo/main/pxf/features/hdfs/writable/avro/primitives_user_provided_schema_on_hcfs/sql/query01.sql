-- @description query01 for PXF HDFS Writable Avro with user-provided schema on HCFS, primitive types

-- start_matchsubs
--
-- # create a match/subs
--
-- end_matchsubs
SELECT * from writable_avro_primitive_user_provided_schema_on_hcfs_readable ORDER BY type_int;
