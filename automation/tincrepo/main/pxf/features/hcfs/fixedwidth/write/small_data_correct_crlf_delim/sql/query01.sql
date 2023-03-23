-- @description query01 tests reading a small data set from fixed width text files with CRLF line delimiter

select * from fixedwidth_out_small_correct_crlf_delim_read order by s1;
