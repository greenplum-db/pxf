-- @description query01 for PXF HDFS Readable Avro with logical types with Incorrect Schema test

-- start_matchsubs
--
-- # create a match/subs
--
-- m/, line \d+ of/
-- s/, line \d+ of .*//
--
-- # FDW in CSV mode will fail during parsing on C side with the below error
-- m/invalid input syntax for date: .*/
-- s/invalid input syntax for date: .*/For field dob schema requires type DATE but input record has type INTEGER/
--
-- end_matchsubs
select * from logical_incorrect_schema_test;
