-- @description query01 tests glob to match a single character from a character set
-- a.[ch]?? will match a.cpp, a.hlp, a.hxy, but it will not match a.c
--

select * from hcfs_glob_match_single_character_from_set order by name, num;
