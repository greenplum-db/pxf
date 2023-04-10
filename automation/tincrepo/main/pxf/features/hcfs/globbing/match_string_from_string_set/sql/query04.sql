-- @description query04 tests glob to match a string from the given set
-- }{a,b}c will match }bc but it will not match }c
--

select * from hcfs_glob_match_string_from_string_set_4 order by name, num;

-- }{b}c will match }bc but it will not match }c

select * from hcfs_glob_match_string_from_string_set_5 order by name, num;

-- }{}bc will match }bc but it will not match }c

select * from hcfs_glob_match_string_from_string_set_6 order by name, num;

-- }{,}bc will match }bc but it will not match }c

select * from hcfs_glob_match_string_from_string_set_7 order by name, num;

-- }{b,}c will match both }bc and }c

select * from hcfs_glob_match_string_from_string_set_8 order by name, num;

-- }{,b}c will match both }bc and }c

select * from hcfs_glob_match_string_from_string_set_9 order by name, num;

-- }{ac,?} will match }c but it will not match }bc

select * from hcfs_glob_match_string_from_string_set_10 order by name, num;

-- error on ill-formed curly

-- start_matchsubs
--
-- # create a match/subs
--
-- m/Illegal file pattern: Unclosed group near index.*/
-- s/Illegal file pattern: Unclosed group near index.*/Unclosed group near index xxx/
--
-- m/java.util.regex.PatternSyntaxException: Unclosed group near index.*/
-- s/java.util.regex.PatternSyntaxException: Unclosed group near index.*/Unclosed group near index xxx/
--
-- m/DETAIL/
-- s/DETAIL/CONTEXT/
--
-- m/pxf:\/\/(.*)\/pxf_automation_data/
-- s/pxf:\/\/.*match_string_from_string_set_4.*/pxf:\/\/pxf_automation_data?PROFILE=*:text/
--
-- m/CONTEXT:.*line.*/
-- s/line \d* of //g
--
-- m/, file pxf:\/\/(.*)/
-- s/, file pxf:\/\/(.*)//
--
-- end_matchsubs

select * from hcfs_glob_match_string_from_string_set_11 order by name, num;

-- }\{bc will match }{bc but it will not match }bc

select * from hcfs_glob_match_string_from_string_set_12 order by name, num;
