-- @description query01 for PXF HDFS Readable Avro with extra field test cases
-- start_matchsubs
--
-- m/, line \d+ of/
-- s/, line \d+ of .*//
--
-- end_matchsubs

SELECT * from avro_extra_field ORDER BY age;
