-- @description query04 tests glob to match any zero or more characters for example
-- */file1 will match dir1/file1, dir3/file1, but it will not match dir2/file2
--

select * from hcfs_glob_match_zero_or_more_characters_4 order by name, num;
