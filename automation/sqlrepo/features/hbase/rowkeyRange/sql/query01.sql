-- @description query01 for PXF HBase - record key range filter

SELECT * from pxf_hbase_table WHERE recordkey > '00000090' AND recordkey <= '00000103' ORDER BY recordkey;