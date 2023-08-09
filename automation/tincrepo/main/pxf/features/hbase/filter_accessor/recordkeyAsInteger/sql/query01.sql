-- @description query01 for PXF HBase  - filter accessor - record key as integer
SET datestyle TO 'ISO, MDY';

SELECT * from hbase_pxf_with_filter ORDER BY recordkey;
