-- @description query01 for PXF HBase different columns name case

-- start_matchsubs
--
-- # create a match/subs
--
-- m/   Description  .*/
-- s/   Description  .*/DESCRIPTION/
--
-- m/DETAIL/
-- s/DETAIL/CONTEXT/
--
-- end_matchsubs
SELECT * from pxf_hbase_different_columns_names ORDER BY recordkey;
