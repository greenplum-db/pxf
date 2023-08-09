-- @description query01 for PXF HBase - record key as integer filter
SET datestyle TO 'ISO, MDY';

SELECT * from pxf_hbase_integer_key WHERE recordkey > 90 AND recordkey <= 103 ORDER BY recordkey;
