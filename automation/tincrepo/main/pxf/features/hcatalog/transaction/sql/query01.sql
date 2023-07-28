-- @description query01 for HCatalog in transaction
BEGIN;
SELECT * FROM hcatalog.default.hive_small_data ORDER BY s1;
SELECT * FROM hcatalog.default.hive_partitioned_table ORDER BY fmt, t0;
SELECT * FROM hcatalog.default.hive_types ORDER BY s1;
END;

SELECT * FROM hcatalog.default.hive_types ORDER BY s1;
SELECT * FROM hcatalog.default.hive_small_data ORDER BY s1;
SELECT * FROM hcatalog.default.hive_partitioned_table ORDER BY fmt, t0;
