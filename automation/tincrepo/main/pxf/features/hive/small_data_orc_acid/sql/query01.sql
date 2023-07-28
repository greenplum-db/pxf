-- @description query01 for PXF Hive Small Data cases with transactional tables
-- start_matchsubs
--
-- # create a match/subs
--
-- m/Check the PXF logs located in the.*/
-- s/Check the PXF logs located in the.*/Check the PXF logs located in the 'log' directory on host 'mdw' or 'set client_min_messages=LOG' for additional details./
--
-- m/DETAIL/
-- s/DETAIL/CONTEXT/
--
-- end_matchsubs

SELECT * from pxf_hive_small_data_orc_acid ORDER BY t1;
