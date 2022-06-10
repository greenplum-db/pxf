-- @description query01 for Hive ORC large data set
SELECT COUNT(*) FROM pxf_hive_orc_large_data;
-- query again to make sure there is no error related to cached file metadata (ORC-1065)
SELECT COUNT(*) FROM pxf_hive_orc_large_data;
