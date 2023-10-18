-- @description query01 for PXF Hive partitioned transactional table cases
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

SELECT * FROM pxf_hive_partitioned_table_acid ORDER BY fmt, t1;

SELECT * FROM pxf_hive_partitioned_table_acid WHERE fmt = 'abcd' ORDER BY fmt, t1;

SELECT * FROM pxf_hive_partitioned_table_acid WHERE fmt IS NULL ORDER BY fmt, t1;
