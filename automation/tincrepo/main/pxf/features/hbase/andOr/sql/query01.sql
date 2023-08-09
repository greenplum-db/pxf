-- @description query01 for PXF HBase - OR AND filter
SET datestyle TO 'ISO, MDY';

SELECT * from pxf_hbase_table  WHERE (((recordkey > '00000090') AND (recordkey <= '00000103')) OR (recordkey = '00000005')) ORDER BY recordkey;
