-- @description query01 for PXF HDFS Readable error when an unterminated quoted
-- field at the end of the file is being read from a PXF external table with
-- SEGMENT REJECT LIMIT for the table definition. A segmentation fault was
-- being thrown by PXF. This test makes sure that the segfault does not occur.
-- start_matchsubs
-- m/ +:/
-- s/( +):/\1|/
-- m/\+$/
-- s/\+$//
-- end_matchsubs

SELECT * FROM unterminated_quoted_field ORDER BY name ASC;
