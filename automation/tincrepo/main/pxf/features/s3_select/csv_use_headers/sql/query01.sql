-- start_ignore
-- end_ignore
-- @description query01 test CSV with headers, using header names to access data, and no compression
--

SELECT l_orderkey, l_partkey, l_commitdate FROM s3select_csv_use_headers WHERE l_orderkey = 194 OR l_orderkey = 82756;
