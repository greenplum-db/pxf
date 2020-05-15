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
-- m/(ERROR|WARNING):.*remote component error.*\(\d+\).*from.*'\d+\.\d+\.\d+\.\d+:\d+'.*/
-- s/'\d+\.\d+\.\d+\.\d+:\d+'/'SOME_IP:SOME_PORT'/
-- m/(ERROR|WARNING):.*remote component error.*\(\d+\).*from.*'(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])):\d+'.*/
-- s/'(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])):\d+'/'SOME_IP:SOME_PORT'/
--
-- m/Exception Report.*(java.io.IOException: Illegal file pattern: Unclosed group near index).*/
-- s/Report.*/Illegal file pattern: Unclosed group near index xxx/
--
-- m/Exception Report.*(java.util.regex.PatternSyntaxException: Unclosed group near index).*/
-- s/Report.*/Illegal file pattern: Unclosed group near index xxx/
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
-- end_matchsubs

select * from hcfs_glob_match_string_from_string_set_11 order by name, num;

-- }\{bc will match }{bc but it will not match }bc

select * from hcfs_glob_match_string_from_string_set_12 order by name, num;
