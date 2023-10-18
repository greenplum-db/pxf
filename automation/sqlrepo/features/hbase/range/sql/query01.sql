-- @description query01 for PXF HBase - range filter

SELECT * from pxf_hbase_table WHERE "cf1:q3" > '00000090' AND "cf1:q3" <= '00000103' ORDER BY recordkey;