-- @description query01 for PXF HBase - lower filter
SET datestyle TO 'ISO, MDY';

SELECT * from pxf_hbase_table WHERE "cf1:q3" < '00000030' ORDER BY recordkey;
