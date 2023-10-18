-- @description query01 for PXF Hive Small Data cases

SELECT t1, t2, num1::int, dub1::double precision from pxf_hive_small_data where num1::int <= 10 ORDER BY t1;