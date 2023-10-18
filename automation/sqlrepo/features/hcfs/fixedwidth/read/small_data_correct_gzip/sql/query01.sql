-- @description query01 tests reading a small data set from fixed width text file compressed with gzip

select * from fixedwidth_in_small_correct_gzip order by s1;
