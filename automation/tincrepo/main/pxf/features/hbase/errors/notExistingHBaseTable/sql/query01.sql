-- @description query01 for PXF HBase not existing HBase table

-- start_matchsubs
--
-- # create a match/subs
--
-- m/DETAIL/
-- s/DETAIL/CONTEXT/
--
-- end_matchsubs
SELECT * from pxf_not_existing_hbase_table ORDER BY recordkey;
