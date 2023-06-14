-- @description query01 for PXF HDFS Writable Json primitive types written in the rows layout

-- start_matchsubs
--
-- # create a match/subs
--
-- end_matchsubs

\pset null 'NIL'
SET bytea_output = 'hex';

SELECT * from gpdb_primitive_types ORDER BY id;

SELECT * from pxf_primitive_types_rows_json_read ORDER BY id;
