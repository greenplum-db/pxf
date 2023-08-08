-- @description query01 reads from the parquet file from a UTF-8 database
set datestyle to 'ISO, MDY';

select * from db_encoding_read_utf8 order by id;

-- Test predicate pushdown
select * from db_encoding_read_utf8 where name = 'однако' order by id;

