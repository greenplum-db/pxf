-- start_ignore
-- end_ignore
-- @description query01 tests that a single line text file returns as a single
-- record in GPDB
--

select * from file_as_row_singleline_text;
