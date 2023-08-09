-- @description query01 for PXF HBase  - filter accessor - record key range
SET datestyle TO 'ISO, MDY';

SELECT * from hbase_pxf_with_filter ORDER BY recordkey;
