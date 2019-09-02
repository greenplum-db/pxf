-- start_ignore
-- end_ignore
-- @description query01 tests that a multiline text file returns as a single multiline record in GPDB
--

-- Display on for output consistency between GPDB 5 and 6
\x on
\pset format unaligned
select * from file_as_row_text;
