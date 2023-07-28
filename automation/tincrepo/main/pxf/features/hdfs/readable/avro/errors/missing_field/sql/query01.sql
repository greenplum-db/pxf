-- @description query01 for PXF HDFS Readable Avro with missing field test cases
-- start_matchsubs
--
-- m/, line \d+ of/
-- s/, line \d+ of .*//
--
-- end_matchsubs

SELECT * from avro_missing_field;
