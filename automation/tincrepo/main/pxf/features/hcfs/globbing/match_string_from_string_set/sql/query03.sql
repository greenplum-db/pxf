-- @description query03 tests glob to match a string from the given set
-- {a/b,c/d} will match a/b, c/d, but it will not match a/d, c/b
--

select * from hcfs_glob_match_string_from_string_set_3 order by name, num;
