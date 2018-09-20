-- @description query01 for PXF HDFS Readable wrong type

-- start_matchsubs
--
-- m/DETAIL/
-- s/DETAIL/CONTEXT/
--
-- m/pxf:\/\/(.*)\/pxf_automation_data/
-- s/pxf:\/\/.*\/pxf_automation_data/pxf:\/\/pxf_automation_data/
--
-- end_matchsubs

SELECT * FROM bad_text ORDER BY num ASC;
