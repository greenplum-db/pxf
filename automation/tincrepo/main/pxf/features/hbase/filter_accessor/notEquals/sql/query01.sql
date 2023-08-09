-- @description query01 for PXF HBase  - filter accessor - not equals
SET datestyle TO 'ISO, MDY';

SELECT * from hbase_pxf_with_filter ORDER BY recordkey;
