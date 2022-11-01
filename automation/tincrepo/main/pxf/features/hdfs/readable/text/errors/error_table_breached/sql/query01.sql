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
-- s/pxf:\/\/.*,/pxf-location,/
--
-- m/pxf_automation_data/
-- s/of .*,/of pxf-location,/
--
-- m/column num:/
-- s/column num:.*/column num/
--
-- end_matchsubs

SELECT * FROM err_table_test ORDER BY num ASC;
