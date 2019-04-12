-- @description query01 for PXF Hive partitioned table cases using union all queries

SELECT * FROM pxf_hive_partitioned_table WHERE fmt = 'avro'
UNION ALL
SELECT * FROM pxf_hive_partitioned_table WHERE fmt = 'rc'
UNION ALL
SELECT * FROM pxf_hive_partitioned_table WHERE fmt = 'txt'
UNION ALL
SELECT * FROM pxf_hive_partitioned_table WHERE fmt = 'seq'
ORDER BY t1;