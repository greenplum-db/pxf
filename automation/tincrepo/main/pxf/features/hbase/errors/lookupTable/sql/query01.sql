-- @description query01 for PXF HBase negative test - lookup table

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
SELECT * from pxf_hbase_table ORDER BY recordkey;

SELECT COUNT(*) FROM pxf_hbase_full_names;
