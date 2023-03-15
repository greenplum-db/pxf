-- @description query01 tests reading a small data set from fixed width text file with CRLF line delimiter

select * from fixed_in_small_correct_crlf_delim_header order by s1;
