-- @description query02 for HCatalog table with multiple partitions - queries for null values
SELECT * FROM hcatalog.default.hive_many_partitioned_table WHERE s2 is null ORDER BY s1;

SELECT * FROM hcatalog.default.hive_many_partitioned_table WHERE n1 is null ORDER BY s1;

SELECT * FROM hcatalog.default.hive_many_partitioned_table WHERE d1 is null ORDER BY s1;

SELECT * FROM hcatalog.default.hive_many_partitioned_table WHERE dc1 is null ORDER BY s1;

SELECT * FROM hcatalog.default.hive_many_partitioned_table WHERE tm is null ORDER BY s1;

SELECT * FROM hcatalog.default.hive_many_partitioned_table WHERE f is null ORDER BY s1;

SELECT * FROM hcatalog.default.hive_many_partitioned_table WHERE bg is null ORDER BY s1;

SELECT * FROM hcatalog.default.hive_many_partitioned_table WHERE b is null ORDER BY s1;

SELECT * FROM hcatalog.default.hive_many_partitioned_table WHERE tn is null ORDER BY s1;

SELECT * FROM hcatalog.default.hive_many_partitioned_table WHERE sml is null ORDER BY s1;

SELECT * FROM hcatalog.default.hive_many_partitioned_table WHERE dt is null ORDER BY s1;

SELECT * FROM hcatalog.default.hive_many_partitioned_table WHERE vc1 is null ORDER BY s1;

SELECT * FROM hcatalog.default.hive_many_partitioned_table WHERE c1 is null ORDER BY s1;

SELECT * FROM hcatalog.default.hive_many_partitioned_table WHERE bin is null ORDER BY s1;
