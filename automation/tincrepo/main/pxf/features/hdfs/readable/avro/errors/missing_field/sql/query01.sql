-- @description query01 for PXF HDFS Readable Avro with missing field test cases

-- start_matchsubs
--
-- # create a match/subs
--
-- m/DETAIL/
-- s/DETAIL/CONTEXT/
--
-- end_matchsubs
SELECT * from avro_missing_field;
