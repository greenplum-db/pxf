-- @description query01 for PXF Hive feature checking error when Hive metastore is down.

-- start_matchsubs
--                                                                                               
-- # create a match/subs
--
-- m/DETAIL/
-- s/DETAIL/CONTEXT/
--
-- end_matchsubs
SELECT *  FROM pxf_hive_metastore_down;