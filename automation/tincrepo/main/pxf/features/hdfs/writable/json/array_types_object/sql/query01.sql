-- @description query01 for PXF HDFS Writable Json array types written in the object layout

-- start_matchsubs
--
-- # create a match/subs
--
-- end_matchsubs

\pset null 'NIL'
SET bytea_output = 'hex';
SET TIME ZONE 'America/Los_Angeles';
SHOW TIME ZONE;
SET datestyle TO 'ISO, MDY';

SELECT id, name, sml, integ, bg, r, dp, dec, bool, cdate, ctime, tm, tmz, c1, vc1, bin from gpdb_array_types ORDER BY id;

SELECT id, name, sml, integ, bg, r, dp, dec, bool, cdate, ctime, tm, tmz, c1, vc1, bin from pxf_array_types_object_json_read ORDER BY id;
