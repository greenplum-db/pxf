-- @description query01 for PXF hive RC read text

-- start_matchsubs
--                                                                                               
-- # create a match/subs
--
-- m/\d+\.\d+\.\d+\.\d+:\d+/
-- s/\d+\.\d+\.\d+\.\d+:\d+/SOME_IP:SOME_PORT/g
--
-- m/   description   .*/
-- s/description   .*/description   DESCRIPTION/
--
-- m/pxf:\/\//
-- s/pxf:\/\/.*hive_small_data/pxf:\/\/ADDRESS\/hive_small_data/g
--
-- m/(ERROR|WARNING):.*javax.servlet.ServletException: java.lang.Exception:.*/
-- s/javax.servlet.ServletException: //
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

SELECT * from pxf_hive_text ORDER BY t1;
