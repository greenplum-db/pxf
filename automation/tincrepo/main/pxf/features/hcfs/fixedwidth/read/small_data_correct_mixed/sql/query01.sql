-- @description query01 tests reading a small data set from fixed width text files with and without compression
set datestyle to 'ISO, MDY';

select * from fixedwidth_in_small_correct_mixed order by s1;
