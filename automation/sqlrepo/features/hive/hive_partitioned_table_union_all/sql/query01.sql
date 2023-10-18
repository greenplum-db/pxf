-- @description query01 for PXF Hive partitioned table cases using union all queries

SELECT DISTINCT t1, fmt FROM pxf_hive_partitioned_table WHERE fmt = 'avro'
UNION ALL
SELECT DISTINCT t1, fmt FROM pxf_hive_partitioned_table WHERE fmt = 'rc'
UNION ALL
SELECT DISTINCT t1, fmt FROM pxf_hive_partitioned_table WHERE fmt = 'txt'
UNION ALL
SELECT DISTINCT t1, fmt FROM pxf_hive_partitioned_table WHERE fmt = 'seq'
ORDER BY fmt, t1;