-- @description query01 for HCatalog partitioned skewed and stored as directories table cases
SELECT * FROM hcatalog.default.hive_partitioned_skewed_stored_table ORDER BY fmt, t0;
