-- @description query01 for PXF HBase - specific row filter
SET datestyle TO 'ISO, MDY';

SELECT * from pxf_hbase_table WHERE "cf1:q3" = 4;
