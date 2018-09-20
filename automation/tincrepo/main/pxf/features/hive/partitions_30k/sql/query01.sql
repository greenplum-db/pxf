-- @description query01 for PXF Hive table with 30k partitions

-- fast test - reads only one fragment
SELECT * FROM hive_30k_parts WHERE date_i = '2015-01-01' ORDER BY i;

-- slow test - reads 30,001 fragments
SELECT * FROM hive_30k_parts ORDER BY i;
