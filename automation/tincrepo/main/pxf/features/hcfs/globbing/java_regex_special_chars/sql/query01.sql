-- @description query01 tests java regex special characters
-- ($.|+)* will match ($.|+)bc but it will not match abc
--

select * from hcfs_glob_java_regex_special_chars order by name, num;
