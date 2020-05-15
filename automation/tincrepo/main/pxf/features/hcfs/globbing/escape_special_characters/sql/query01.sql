-- @description query01 tests removing (escaping) any special meaning of a character
-- ab\[c.d will match ab[c.d
--

select * from hcfs_glob_escape_special_characters order by name, num;
