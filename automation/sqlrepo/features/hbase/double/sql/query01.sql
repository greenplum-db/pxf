-- @description query01 for PXF HBase - double filter

SELECT * from pxf_hbase_table WHERE "cf1:q5" > 91.92 AND "cf1:q6" <= 98989898.98;