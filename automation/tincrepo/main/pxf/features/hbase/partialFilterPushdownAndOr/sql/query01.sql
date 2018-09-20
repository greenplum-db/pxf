-- @description query01 for PXF HBase - partial filter pushdown

SELECT * from pxf_hbase_table WHERE (recordkey > '00000001') AND ((recordkey <= '00000093') AND (recordkey >= '00000080') OR recordkey = '0') ORDER BY recordkey;
