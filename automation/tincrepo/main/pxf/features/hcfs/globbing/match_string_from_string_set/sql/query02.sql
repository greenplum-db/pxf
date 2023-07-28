-- @description query02 tests glob to match a string from the given set
-- a.{ab{c,d},jh}?? will match a.abcxx, a.abdxy, a.jhyy but it will not match a.hlp
--

select * from hcfs_glob_match_string_from_string_set_2 order by name, num;
