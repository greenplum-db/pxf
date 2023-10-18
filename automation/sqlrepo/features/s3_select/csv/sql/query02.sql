-- start_ignore
-- end_ignore
-- @description query02 test S3 Select access to CSV with headers and no compression
--

-- test filters with varchar, char and numeric types
-- while we can not prove here they have actually been pushed down to S3
-- we can prove the query does not fail if they are used
SELECT l_orderkey, l_quantity, l_shipmode, l_comment FROM s3select_csv
WHERE  l_orderkey < 2000 AND (l_quantity = 15 AND l_shipmode = 'RAIL' OR l_comment = 'ideas doubt')
ORDER BY l_orderkey;
