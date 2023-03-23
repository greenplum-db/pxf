-- @description query01 tests reading a small data set from fixed width text file with CRLF line delimiter

select * from fixedwidth_in_small_correct_crlf_delim order by s1;
