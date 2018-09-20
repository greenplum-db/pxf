-- @description query01 for Hive aggregate query when one file is divided into multiple splits
SELECT COUNT(*) FROM pxf_hive_small_data_multiple_fragments_per_file;