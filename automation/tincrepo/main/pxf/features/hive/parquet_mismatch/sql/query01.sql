-- @description query01 for PXF Hive parquet mismatch

SELECT * FROM pxf_hive_parquet_mismatch ORDER BY num;

-- column projection on non-part column
SELECT dcm FROM pxf_hive_parquet_mismatch ORDER BY dcm;

-- column projection on non-part and part columns
SELECT part, s1 FROM pxf_hive_parquet_mismatch ORDER BY s1;

-- column projection on part column
SELECT part FROM pxf_hive_parquet_mismatch ORDER BY part;

-- column projection and predicate pushdown on partially NULL column
SELECT s1 FROM pxf_hive_parquet_mismatch WHERE s2 IS NOT NULL ORDER BY s1;

-- column projection and predicate pushdown
SELECT num, s1 FROM pxf_hive_parquet_mismatch WHERE num > 15 ORDER BY num;

-- column projection and partition pruning
SELECT num, s1 FROM pxf_hive_parquet_mismatch WHERE part='c' ORDER BY num;

-- column projection and partition pruning and pushdown
SELECT num, s1 FROM pxf_hive_parquet_mismatch WHERE num < 27 AND part='b' ORDER BY num;