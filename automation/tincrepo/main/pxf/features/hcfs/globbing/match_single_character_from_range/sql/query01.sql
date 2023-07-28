-- @description query01 tests glob to match a single character from a character range
-- a.[d-f] will match a.d, a.e, a.f, but it will not match a.h
--

select * from hcfs_glob_match_single_character_from_range order by name, num;
