-- @description query01 for PXF HDFS Readable Avro with missing schema file test cases

-- start_matchsubs
--                                                                                               
-- # create a match/subs
--
-- m/ +(d|D)escription  .*/
-- s/ +(d|D)escription  .*/ Description DESCRIPTION/
--
-- m/ +Failed to obtain Avro schema from 'i_do_not_exist'/
-- s/ +Failed to obtain Avro schema from 'i_do_not_exist'/ Failed to obtain Avro schema from 'i_do_not_exist'/
--
-- m/(E|e)xception (r|R)eport +(m|M)essage/
-- s/(E|e)xception (r|R)eport +(m|M)essage/exception report message/
--
-- end_matchsubs
SELECT * from avro_in_seq_no_schema;
