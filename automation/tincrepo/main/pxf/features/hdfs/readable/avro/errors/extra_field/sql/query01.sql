-- @description query01 for PXF HDFS Readable Avro with extra field test cases

-- start_matchsubs
--
-- # create a match/subs
--
-- m/DETAIL/
-- s/DETAIL/CONTEXT/
--
-- end_matchsubs
SELECT * from avro_extra_field ORDER BY age;
