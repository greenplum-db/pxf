-- @description query01 tests reading a small data set from fixed width text files compressed with gzip
set datestyle to 'ISO, MDY';

select * from fixedwidth_out_small_correct_gzip_read order by s1;
