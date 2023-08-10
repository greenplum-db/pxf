-- @description query01 tests reading a small data set from fixed width text file with custom record delimiter
set datestyle to 'ISO, MDY';

select * from fixedwidth_out_small_correct_custom_delim_read order by s1;
