-- @description query01 tests glob to match a single character that is not from
-- character set or range {a}. Note that the "^" character must occur
-- immediately to the right of the opening bracket.
-- a.[^a-cg-z0-9] will match a.d, a.e, but it will not match a.0, a.h
--

select * from hcfs_glob_match_single_character_set_exclusion order by name, num;
