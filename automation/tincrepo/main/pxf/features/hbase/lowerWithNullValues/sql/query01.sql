-- @description query01 for PXF HBase - lower with null values filter

SELECT * from pxf_hbase_null_table WHERE "cf1:q3" < 30 ORDER BY recordkey;