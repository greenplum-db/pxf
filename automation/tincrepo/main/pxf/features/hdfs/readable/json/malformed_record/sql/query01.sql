-- start_matchsubs
-- m/^CONTEXT: */
-- s/^CONTEXT: *//
--
-- m/^DETAIL:*/
-- s/^DETAIL:*//
--
-- end_matchsubs
-- @description query01 for PXF HDFS Readable Json with malformed record test cases
SELECT *
FROM jsontest_malformed_record
ORDER BY id;
SELECT *
FROM jsontest_malformed_record
WHERE id IS NULL
ORDER BY id;

SELECT *
FROM jsontest_malformed_record_filefrag
ORDER BY id;
SELECT *
FROM jsontest_malformed_record_filefrag
WHERE id IS NULL
ORDER BY id;