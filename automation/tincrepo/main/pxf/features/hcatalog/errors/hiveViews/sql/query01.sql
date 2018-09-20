-- @description query01 for HCatalog view table - Negative case

-- start_matchsubs
--                                                                                               
-- # create a match/subs
--
-- m/(ERROR|WARNING):.*remote component error.*\(\d+\).*from.*'\d+\.\d+\.\d+\.\d+:\d+'.*/
-- s/'\d+\.\d+\.\d+\.\d+:\d+'/'SOME_IP:SOME_PORT'/
--
-- m/   description   .*/
-- s/.*/description   DESCRIPTION/
--
-- end_matchsubs
SELECT * FROM hcatalog.default.hive_small_data_view;
