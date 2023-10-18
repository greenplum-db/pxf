-- @description query01 for PXF hive column datatype mis match

-- start_matchsubs
--                                                                                               
-- # create a match/subs
--
-- m/(ERROR|WARNING):.*  External table definition did not match input data: column.*seg.*/
-- s/seg.*//
--
-- m/DETAIL/
-- s/DETAIL/CONTEXT/
--
-- end_matchsubs
SELECT * from pxf_hive_small_data ORDER BY t1;