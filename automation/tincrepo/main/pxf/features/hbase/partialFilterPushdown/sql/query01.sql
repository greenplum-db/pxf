-- @description query01 for PXF HBase - partial filter pushdown
SET datestyle TO 'ISO, MDY';

SELECT * from pxf_hbase_table WHERE "cf1:q3" > 6  AND "cf1:q10" = 'f' ORDER BY recordkey;
