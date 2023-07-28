-- @description query01 for PXF HDFS Writable Json primitive types escaping

-- start_matchsubs
--
-- # create a match/subs
--
-- end_matchsubs

SELECT * from pxf_primitive_types_escaping_json_read ORDER BY id;
