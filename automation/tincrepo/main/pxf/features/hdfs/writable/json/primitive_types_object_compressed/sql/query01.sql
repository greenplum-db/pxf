-- @description query01 for PXF HDFS Writable Json primitive types written in the object layout with compression

-- start_matchsubs
--
-- # create a match/subs
--
-- end_matchsubs

\pset null 'NIL'
SET bytea_output = 'hex';
SET datestyle TO 'ISO, MDY';

SELECT id, name, sml, integ, bg, r, dp, dec, bool, cdate, ctime, tm, CAST(tmz AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'PDT' as tmz, c1, vc1, bin from gpdb_primitive_types ORDER BY id;

SELECT id, name, sml, integ, bg, r, dp, dec, bool, cdate, ctime, tm, CAST(tmz AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'PDT' as tmz, c1, vc1, bin from pxf_primitive_types_object_compressed_json_read ORDER BY id;
