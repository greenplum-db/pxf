-- @description query01 tests reading a small data set from fixed width text file
set datestyle to 'ISO, MDY';

select * from fixedwidth_in_small_correct order by s1;
