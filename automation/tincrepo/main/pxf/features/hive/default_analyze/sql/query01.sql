-- @description query01 for PXF Hive default analyze cases

SELECT relpages, reltuples FROM pg_class  WHERE relname = 'pxf_hive_small_data';