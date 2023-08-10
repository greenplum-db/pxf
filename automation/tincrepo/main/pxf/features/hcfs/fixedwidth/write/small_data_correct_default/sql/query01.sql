-- @description query01 tests reading a small data set from fixed width text file
set datestyle to 'ISO, MDY';

select * from fixedwidth_out_small_correct_default_read order by s1;
