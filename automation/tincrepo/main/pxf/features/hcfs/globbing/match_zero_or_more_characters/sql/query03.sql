-- @description query03 tests glob to match any zero or more characters for example
-- a*x will match a.txt.x, ax, ab37x, but it will not match bacd
--

select * from hcfs_glob_match_zero_or_more_characters_3 order by name, num;
