-- @description query05 tests glob to match any zero or more characters for example
-- */file1 will match dir1/file1 but it will not match file1
--

select * from hcfs_glob_match_zero_or_more_characters_5 order by name, num;
