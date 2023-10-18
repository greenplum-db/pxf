-- @description query01 for PXF HDFS Writable Json where table is defined with an invalid (non-UTF8) encoding

-- start_matchsubs
--
-- # create a match/subs
--
-- end_matchsubs

INSERT INTO pxf_invalid_encoding_json_write SELECT * from gpdb_primitive_types;
