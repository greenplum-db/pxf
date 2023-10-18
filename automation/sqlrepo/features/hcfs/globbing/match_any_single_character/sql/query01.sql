-- @description query01 tests glob to match any single character for example
-- a?c will match abc, a2c, a.c, but it will not match abcd
--

select * from hcfs_glob_match_any_single_character order by name, num;
