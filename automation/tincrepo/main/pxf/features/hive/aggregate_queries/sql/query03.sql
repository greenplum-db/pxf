-- @description query03 for Hive aggregate query
SELECT COUNT(*) FROM pxf_hive_small_data WHERE dub1 >= 0;
SELECT COUNT(*) FROM pxf_hive_small_data WHERE num1 > 10;
SELECT COUNT(*) FROM pxf_hive_small_data WHERE dub1 <= 15 AND num1 >= 9;