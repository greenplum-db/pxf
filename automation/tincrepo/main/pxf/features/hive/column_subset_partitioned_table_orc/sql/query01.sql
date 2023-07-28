-- @description query01 for PXF Hive Small Column subset test

SELECT * from pxf_hive_small_data WHERE num1 <= 10 ORDER BY num1;
