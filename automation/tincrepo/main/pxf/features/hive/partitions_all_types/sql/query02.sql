-- @description query02 for PXF Hive table with multiple partitions - queries for null values
SET datestyle TO 'ISO, MDY';

SELECT * FROM hive_partitions_all_types WHERE t2 is null ORDER BY t1;

SELECT * FROM hive_partitions_all_types WHERE num1 is null ORDER BY t1;

SELECT * FROM hive_partitions_all_types WHERE dub1 is null ORDER BY t1;

SELECT * FROM hive_partitions_all_types WHERE dec1 is null ORDER BY t1;

SELECT * FROM hive_partitions_all_types WHERE tm is null ORDER BY t1;

SELECT * FROM hive_partitions_all_types WHERE r is null ORDER BY t1;

SELECT * FROM hive_partitions_all_types WHERE bg is null ORDER BY t1;

SELECT * FROM hive_partitions_all_types WHERE b is null ORDER BY t1;

SELECT * FROM hive_partitions_all_types WHERE tn is null ORDER BY t1;

SELECT * FROM hive_partitions_all_types WHERE sml is null ORDER BY t1;

SELECT * FROM hive_partitions_all_types WHERE dt is null ORDER BY t1;

SELECT * FROM hive_partitions_all_types WHERE vc1 is null ORDER BY t1;

SELECT * FROM hive_partitions_all_types WHERE c1 is null ORDER BY t1;

SELECT * FROM hive_partitions_all_types WHERE bin is null ORDER BY t1;
