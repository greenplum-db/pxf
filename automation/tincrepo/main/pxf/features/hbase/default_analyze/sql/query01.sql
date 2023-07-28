-- @description query01 for PXF HBase default analyze cases

SELECT relpages, reltuples FROM pg_class  WHERE relname = 'pxf_hbase_table';