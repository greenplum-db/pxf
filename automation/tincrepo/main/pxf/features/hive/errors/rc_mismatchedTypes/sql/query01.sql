-- @description query01 for PXF hive RC mismatched types

-- start_matchsubs
--                                                                                               
-- # create a match/subs
--
-- m/  Description  .*/
-- s/Description  .*/Description   DESCRIPTION/
--
-- m/DETAIL/
-- s/DETAIL/CONTEXT/
--
-- m/CONTEXT:.*line.*/
-- s/line \d* of //g
--
-- m//
-- s///
--
-- end_matchsubs

SELECT * from gpdb_hive_types ORDER BY t1;
