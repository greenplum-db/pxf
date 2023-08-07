-- @description query01 for PXF Hive parquet timestamp
SET datestyle TO 'ISO, MDY';
SELECT * FROM pxf_hive_parquet_timestamp ORDER BY tm;
