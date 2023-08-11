-- @description query01 tests reading a small data set from fixed width text file with CR line delimiter
--
-- start_matchsubs
--
-- m/.*WARNING.*/
-- s/^.*\n//
--
-- m/^CONTEXT.*/
-- s/CONTEXT.*//
--
-- end_matchsubs
set datestyle to 'ISO, MDY';
select * from fixedwidth_in_small_correct_cr_delim order by s1;
