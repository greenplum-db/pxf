-- @description query01 for PXF HDFS Readable wrong type

-- start_matchsubs
--
-- m/DETAIL/
-- s/DETAIL/CONTEXT/
--
-- m/ERROR/
-- s/ERROR:  invalid input syntax for integer: "joker"/ERROR:  invalid input syntax for type integer: "joker"/g
--
-- m/pxf:\/\/(.*),/
-- s/pxf:\/\/.*,/pxf:\/\/location,/
--
-- m/, line 51 of/
-- s/, line 51 of.*//
--
-- end_matchsubs

SELECT * FROM bad_text ORDER BY num ASC;
