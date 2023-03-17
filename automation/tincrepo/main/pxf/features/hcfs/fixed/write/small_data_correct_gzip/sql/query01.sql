-- @description query01 tests reading a small data set from fixed width text files compressed with gzip

select * from fixed_out_small_correct_gzip_read order by s1;
