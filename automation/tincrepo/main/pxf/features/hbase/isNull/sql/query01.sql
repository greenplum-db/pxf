-- @description query01 for PXF HBase - is null filter

SELECT * FROM pxf_hbase_null_table WHERE "cf1:q3" is null;