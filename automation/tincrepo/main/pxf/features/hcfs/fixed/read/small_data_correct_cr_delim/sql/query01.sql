-- @description query01 tests reading a small data set from fixed width text file with CR line delimiter
--
-- start_matchsubs
--
-- m/GP_IGNORE: WARNING.*/
-- s/GP_IGNORE: WARNING/WARNING/
--
-- end_matchsubs
select * from fixed_in_small_correct_cr_delim order by s1;
