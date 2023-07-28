-- @description query02 tests glob to match any zero or more characters for example
-- a.* will match a., a.txt, a.old.java, but it will not match .java
--

select * from hcfs_glob_match_zero_or_more_characters_2 order by name, num;
