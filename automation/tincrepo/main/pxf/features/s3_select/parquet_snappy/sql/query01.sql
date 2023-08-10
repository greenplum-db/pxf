-- start_ignore
-- end_ignore
-- @description query01 test S3 Select access to Parquet and snappy columnar compression
--
SET datestyle TO 'ISO, MDY';

SELECT l_orderkey, l_partkey, l_commitdate FROM s3select_parquet_snappy WHERE l_orderkey = 194 OR l_orderkey = 82756;
