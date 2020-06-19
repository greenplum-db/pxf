-- @description query01 for PXF hive column count mismatch

-- start_matchsubs
--                                                                                               
-- # create a match/subs
--
-- m/   description   .*/
-- s/description   .*/description   DESCRIPTION/
--
-- m/DETAIL/
-- s/DETAIL/CONTEXT/
--
-- end_matchsubs
SELECT * from pxf_hive_small_data ORDER BY t1;