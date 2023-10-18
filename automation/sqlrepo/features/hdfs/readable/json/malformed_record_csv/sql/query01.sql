-- start_matchsubs
--
-- m/DETAIL/
-- s/DETAIL/CONTEXT/
--
-- m/CONTEXT:.*line.*/
-- s/line \d* of //g
--
-- m/^CONTEXT: */
-- s/^CONTEXT: *//
--
-- m/pxf:\/\/(.*)\/pxf_automation_data/
-- s/pxf:\/\/.*\/pxf_automation_data.*tweets-broken.json\?PROFILE=.*:json/pxf:\/\/pxf_automation_data\/tweets-broken.json?PROFILE=json/
--
-- end_matchsubs
-- @description query01 for PXF HDFS Readable Json with malformed record test cases
SELECT *
FROM jsontest_malformed_record
ORDER BY id;

SELECT *
FROM jsontest_malformed_record_filefrag
ORDER BY id;