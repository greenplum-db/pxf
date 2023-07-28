-- @description query01 for PXF Hive table with multiple partitions

SELECT * FROM hive_partitions_all_types ORDER BY t1;

SELECT * FROM hive_partitions_all_types WHERE t2 = 's_16'  ORDER BY t1;

SELECT * FROM hive_partitions_all_types WHERE num1 = 10 ORDER BY t1;

SELECT * FROM hive_partitions_all_types WHERE dub1 = 37 ORDER BY t1;

SELECT * FROM hive_partitions_all_types WHERE dec1 = 0.123456789012345679 ORDER BY t1;

SELECT * FROM hive_partitions_all_types WHERE tm = '2013-07-23 21:00:05' ORDER BY t1;

SELECT * FROM hive_partitions_all_types WHERE r::numeric = 7.7 ORDER BY t1;

SELECT * FROM hive_partitions_all_types WHERE bg = 23456789 ORDER BY t1;

SELECT * FROM hive_partitions_all_types WHERE b = 'true' ORDER BY t1;

SELECT * FROM hive_partitions_all_types WHERE tn = 4 ORDER BY t1;

SELECT * FROM hive_partitions_all_types WHERE sml = 1100 ORDER BY t1;

SELECT * FROM hive_partitions_all_types WHERE dt = '2015-03-06' ORDER BY t1;

SELECT * FROM hive_partitions_all_types WHERE vc1 = 'abcde' ORDER BY t1;

SELECT * FROM hive_partitions_all_types WHERE c1 = 'abc' ORDER BY t1;

SELECT * FROM hive_partitions_all_types WHERE bin = '31'::bytea ORDER BY t1;
