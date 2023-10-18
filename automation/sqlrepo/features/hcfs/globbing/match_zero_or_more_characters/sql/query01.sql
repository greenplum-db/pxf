-- @description query01 tests glob to match any zero or more characters for example
-- a* will match a, abc, abc.p, but it will not match bacd
--

select * from hcfs_glob_match_zero_or_more_characters_1 order by name, num;
