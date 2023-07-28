-- @description query01 for PXF HDFS Readable error in the middle of stream

-- start_matchsubs
--
-- m/(ERROR|WARNING):.*'\d+\.\d+\.\d+\.\d+:\d+'.*/
-- s/'\d+\.\d+\.\d+\.\d+:\d+'/'SOME_IP:SOME_PORT'/
--
-- m/CONTEXT:  \n/
-- s/CONTEXT:  \n/CONTEXT:  /
--
-- m/External table error_on_10000/
-- s/.*External table error_on_10000.*/CONTEXT:  External table error_on_10000/
--
-- end_matchsubs

SELECT * FROM error_on_10000 ORDER BY num ASC;
