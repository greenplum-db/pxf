-- @description query01 for PXF hive RC unsupported types

-- start_matchsubs
--                                                                                               
-- # create a match/subs
--
-- m/\d+\.\d+\.\d+\.\d+:\d+/
-- s/\d+\.\d+\.\d+\.\d+:\d+/SOME_IP:SOME_PORT/g
--
-- m/   description   .*/
-- s/description   .*/description   DESCRIPTION/
--
-- end_matchsubs

SELECT * from pxf_hive_collections ORDER BY t1;