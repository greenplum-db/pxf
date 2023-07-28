-- @description query01 tests glob to match a string from the given set
-- a.{abc,jh}?? will match a.abcxx, a.jhyy, but it will not match a.abxy, a.hlp
--

select * from hcfs_glob_match_string_from_string_set_1 order by name, num;
