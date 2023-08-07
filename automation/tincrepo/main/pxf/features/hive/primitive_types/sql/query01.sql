-- @description query01 for PXF Hive primitive types case
SET datestyle TO 'ISO, MDY';
SELECT * FROM gpdb_hive_types ORDER BY t1;
