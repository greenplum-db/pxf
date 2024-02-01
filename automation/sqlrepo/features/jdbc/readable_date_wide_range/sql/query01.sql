-- @description query01 for JDBC query with date wide range on
--
-- start_matchsubs
--
-- # create a match/subs
--
-- m/(.*)ERROR:/
-- s/(.*)ERROR:/ERROR:/
--
-- m/ERROR:  invalid byte sequence for encoding.*/
-- s/ERROR:  invalid byte sequence for encoding.*/ERROR:  invalid input syntax./
--
-- m/ERROR:  invalid input syntax.*/
-- s/ERROR:  invalid input syntax.*/ERROR:  invalid input syntax./
--
-- m/ERROR:  time zone displacement out of range.*/
-- s/ERROR:  time zone displacement out of range.*/ERROR:  time zone displacement out of range/
--
-- m/PXF server error.*Field type 'TIMESTAMP_WITH_TIME_ZONE'.*is not supported/
-- s/PXF server error.*/PXF server error : Field type 'TIMESTAMP_WITH_TIME_ZONE' is not supported/
--
-- m/DETAIL/
-- s/DETAIL/CONTEXT/
--
-- m/CONTEXT:.*line.*/
-- s/line \d* of //g
--
-- m/CONTEXT:.*pxf_jdbc_readable_date_wide_range_off/
-- s/CONTEXT:.*pxf_jdbc_readable_date_wide_range_off.*/CONTEXT: External Table pxf_jdbc_readable_date_wide_range_off/
--
-- end_matchsubs

SET timezone='UTC';

SELECT * FROM pxf_jdbc_readable_date_wide_range_on ORDER BY t1;

SELECT tm FROM pxf_jdbc_readable_date_wide_range_off ORDER BY t1;

SELECT dt FROM pxf_jdbc_readable_date_wide_range_off ORDER BY t1;

SELECT tmz FROM pxf_jdbc_readable_date_wide_range_off ORDER BY t1;
