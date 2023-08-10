-- @description query01 tests reading a small data set from fixed width text files with LF line delimiter
set datestyle to 'ISO, MDY';

select * from fixedwidth_out_small_correct_lf_delim_read order by s1;
