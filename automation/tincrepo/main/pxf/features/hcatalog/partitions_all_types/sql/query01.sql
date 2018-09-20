-- @description query01 for HCatalog table with multiple partitions
SELECT * FROM hcatalog.default.hive_many_partitioned_table ORDER BY s1;

SELECT * FROM hcatalog.default.hive_many_partitioned_table WHERE s2 = 's_16'  ORDER BY s1;

SELECT * FROM hcatalog.default.hive_many_partitioned_table WHERE n1 = 10 ORDER BY s1;

SELECT * FROM hcatalog.default.hive_many_partitioned_table WHERE d1 = 37 ORDER BY s1;

SELECT * FROM hcatalog.default.hive_many_partitioned_table WHERE dc1 = 0.123456789012345679 ORDER BY s1;

SELECT * FROM hcatalog.default.hive_many_partitioned_table WHERE tm = '2013-07-23 21:00:05' ORDER BY s1;

SELECT * FROM hcatalog.default.hive_many_partitioned_table WHERE f::numeric = 7.7 ORDER BY s1;

SELECT * FROM hcatalog.default.hive_many_partitioned_table WHERE bg = 23456789 ORDER BY s1;

SELECT * FROM hcatalog.default.hive_many_partitioned_table WHERE b = 'true' ORDER BY s1;

SELECT * FROM hcatalog.default.hive_many_partitioned_table WHERE tn = 4 ORDER BY s1;

SELECT * FROM hcatalog.default.hive_many_partitioned_table WHERE sml = 1100 ORDER BY s1;

SELECT * FROM hcatalog.default.hive_many_partitioned_table WHERE dt = '2015-03-06' ORDER BY s1;

SELECT * FROM hcatalog.default.hive_many_partitioned_table WHERE vc1 = 'abcde' ORDER BY s1;

SELECT * FROM hcatalog.default.hive_many_partitioned_table WHERE c1 = 'abc' ORDER BY s1;

SELECT * FROM hcatalog.default.hive_many_partitioned_table WHERE bin = '31'::bytea ORDER BY s1;
