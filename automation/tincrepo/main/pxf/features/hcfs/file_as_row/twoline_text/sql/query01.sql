-- start_ignore
-- end_ignore
-- @description query01 tests that a two line text file returns correctly
-- record in GPDB
--

-- Display on for output consistency between GPDB 5 and 6
\x on
select * from file_as_row_twoline_text;
