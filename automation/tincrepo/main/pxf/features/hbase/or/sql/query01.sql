-- @description query01 for PXF HBase - OR filter
SET datestyle TO 'ISO, MDY';

SELECT * from pxf_hbase_table WHERE "cf1:q3" < 10 OR "cf1:q5" > 90 ORDER BY recordkey;
