-- @description query01 for PXF Hive RC filter no partitions

SELECT * FROM pxf_hive_heterogen_using_filter WHERE num1 > 5 AND dub1 < 12 ORDER BY fmt, prt;
