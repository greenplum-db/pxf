-- @description query01 for PXF HDFS Readable Avro with logical types with Incorrect Schema test

-- start_matchsubs
--
-- # create a match/subs
--
-- m/DETAIL/
-- s/DETAIL/CONTEXT/
--
-- end_matchsubs
select * from logical_incorrect_schema_test;
