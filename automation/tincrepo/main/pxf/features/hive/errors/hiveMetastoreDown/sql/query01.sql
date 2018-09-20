-- @description query01 for PXF Hive feature checking error when Hive metastore is down.

-- start_matchsubs
--                                                                                               
-- # create a match/subs
--
-- m/(ERROR|WARNING):.*remote component error.*\(\d+\).*from.*'\d+\.\d+\.\d+\.\d+:\d+'.*/
-- s/'\d+\.\d+\.\d+\.\d+:\d+'/'SOME_IP:SOME_PORT'/
--
-- m/   description   .*/
-- s/description   .*/description   DESCRIPTION/
--
-- end_matchsubs
SELECT *  FROM pxf_hive_metastore_down;