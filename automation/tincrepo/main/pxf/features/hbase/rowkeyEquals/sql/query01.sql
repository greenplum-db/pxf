-- @description query01 for PXF HBase - record key equals filter
SET datestyle TO 'ISO, MDY';

SELECT * from pxf_hbase_table WHERE recordkey = '00000090';
