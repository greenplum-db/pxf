-- @description query02 for HCatalog test on small data
SELECT s1, n1 FROM hcatalog.default.hive_table WHERE n1 > 50 ORDER BY s1;
