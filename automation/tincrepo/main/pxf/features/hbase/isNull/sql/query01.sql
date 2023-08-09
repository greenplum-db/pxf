-- @description query01 for PXF HBase - is null filter
SET datestyle TO 'ISO, MDY';

SELECT * FROM pxf_hbase_null_table WHERE "cf1:q3" is null;
