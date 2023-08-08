-- @description query01 reads from the parquet file from a non-UTF-8 database

-- start_ignore
\c pxfautomation_encoding
-- set client encoding to UTF8
\encoding UTF8
-- end_ignore
set datestyle to 'ISO, MDY';

select * from db_encoding_read_other order by id;

-- Test predicate pushdown
select * from db_encoding_read_other where name = 'однако' order by id;

