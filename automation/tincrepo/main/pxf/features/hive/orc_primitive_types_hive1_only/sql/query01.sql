-- @description query01 for HCatalog ORC timestamp column in WHERE clause
SET datestyle TO 'ISO, MDY';
SELECT t1 FROM pxf_hive_orc_types WHERE tm = '2013-07-13 21:00:05';
SELECT t1 FROM pxf_hive_orc_types WHERE '2013-07-13 21:00:05' = tm;
