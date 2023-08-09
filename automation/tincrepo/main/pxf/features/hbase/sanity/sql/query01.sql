-- @description query01 for PXF HBase Small Data sanity case
SET datestyle TO 'ISO, MDY';

SELECT * from pxf_hbase_table ORDER BY recordkey;
