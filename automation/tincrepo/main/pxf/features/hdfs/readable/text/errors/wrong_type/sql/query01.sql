-- @description query01 for PXF HDFS Readable wrong type

-- start_matchsubs
--
-- m/DETAIL/
-- s/DETAIL/CONTEXT/
--
-- m/pxf:\/\/(.*),/
-- s/pxf:\/\/.*,/pxf:\/\/location,/
--
-- end_matchsubs

SELECT * FROM bad_text ORDER BY num ASC;
