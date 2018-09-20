-- @description query01 for PXF hive column count mismatch

-- start_matchsubs
--
-- m/DETAIL/
-- s/DETAIL/CONTEXT/
--
-- end_matchsubs
SELECT * from pxf_hive_small_data ORDER BY t1;