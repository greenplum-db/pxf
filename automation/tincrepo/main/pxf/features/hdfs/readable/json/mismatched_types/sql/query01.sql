-- start_matchsubs
-- m/CONTEXT:/
-- s/CONTEXT:/DETAIL:/
-- end_matchsubs
-- @description query01 for PXF HDFS Readable Json supported primitive types test cases
SELECT *
FROM jsontest_mismatched_types
ORDER BY type_int;