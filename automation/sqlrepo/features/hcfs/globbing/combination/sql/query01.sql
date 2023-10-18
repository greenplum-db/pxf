-- @description query01 tests a combination of glob patterns
-- use?/*/a.[ch]{lp,xy} will match user/dd/a.hxy but it will not match
-- user/aa/a.c, user/bb/a.cpp, user1/cc/b.hlp
--

select * from hcfs_glob_combination order by name, num;
