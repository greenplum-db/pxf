-- @description query01 for PXF HDFS Readable error table breached

-- start_matchsubs
--
-- m/DETAIL/
-- s/DETAIL/CONTEXT/
--
-- m/ERROR:\s*(S|s)egment reject limit reached/
-- s/ERROR:\s*(S|s)egment reject limit reached.*/ERROR: segment reject limit reached/
--
-- m/CONTEXT:\s*Last error was/
-- s/CONTEXT:\s*Last error was/GP_IGNORE:/
--
-- m/pxf:\/\/(.*),/
-- s/pxf:\/\/.*,/pxf:\/\/location,/
--
-- end_matchsubs

SELECT * FROM err_table_test ORDER BY num ASC;
