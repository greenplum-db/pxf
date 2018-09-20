-- @description query01 for PXF HBase - not equals filter

SELECT * from pxf_hbase_table WHERE "cf1:q3" != 30 ORDER BY recordkey;