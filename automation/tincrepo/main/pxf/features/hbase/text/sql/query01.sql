-- @description query01 for PXF HBase - text filter
SET datestyle TO 'ISO, MDY';

SELECT * from pxf_hbase_table WHERE "cf1:q2" = 'UTF8_計算機用語_00000024';
