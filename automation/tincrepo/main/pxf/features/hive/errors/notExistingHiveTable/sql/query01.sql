-- @description query01 for PXF over Hive view table - Negative case

-- start_matchsubs
--                                                                                               
-- # create a match/subs
--
-- m/message:hive.default.no_such_hive_table/
-- s/hive.default.no_such_hive_table/default.no_such_hive_table/g
--
-- m/DETAIL/
-- s/DETAIL/CONTEXT/
--
-- end_matchsubs
SELECT *  FROM pxf_none_hive_table;
