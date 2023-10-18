-- @description query01 for PXF hive invalid filter pushdown string

-- start_matchsubs
--                                                                                               
-- # create a match/subs
--
-- m/\d+\.\d+\.\d+\.\d+:\d+/
-- s/\d+\.\d+\.\d+\.\d+:\d+/SOME_IP:SOME_PORT/g
--
-- m/DETAIL/
-- s/DETAIL/GP_IGNORE: DETAIL/
--
-- m/CONTEXT/
-- s/CONTEXT/GP_IGNORE: CONTEXT/
--
-- end_matchsubs

SELECT *  FROM hive_invalid_filter ORDER BY fmt, t1;
