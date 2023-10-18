-- start_ignore
-- end_ignore
-- @description query01 tests that multiple files are read as a single row each
--

select count(*) from file_as_row_multi_files;

-- Display on for output consistency between GPDB 5 and 6
\x on
\pset format unaligned
select * from file_as_row_multi_files order by text_blob;
