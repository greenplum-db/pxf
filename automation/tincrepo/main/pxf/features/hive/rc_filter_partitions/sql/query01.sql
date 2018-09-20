-- @description query01 for PXF Hive RC filter partitions

SELECT * FROM pxf_hive_heterogen_using_filter WHERE fmt = 'rc1' AND prt = 'a' ORDER BY fmt, t1;
