-- @description query01 for Hive ORC small data multifile vectorized
-- query 10 times to make sure there are no race conditions across fragment processing
SELECT * FROM pxf_hive_orc_multifile_vectorized ORDER BY t1;
SELECT * FROM pxf_hive_orc_multifile_vectorized ORDER BY t1;
SELECT * FROM pxf_hive_orc_multifile_vectorized ORDER BY t1;
SELECT * FROM pxf_hive_orc_multifile_vectorized ORDER BY t1;
SELECT * FROM pxf_hive_orc_multifile_vectorized ORDER BY t1;
SELECT * FROM pxf_hive_orc_multifile_vectorized ORDER BY t1;
SELECT * FROM pxf_hive_orc_multifile_vectorized ORDER BY t1;
SELECT * FROM pxf_hive_orc_multifile_vectorized ORDER BY t1;
SELECT * FROM pxf_hive_orc_multifile_vectorized ORDER BY t1;
SELECT * FROM pxf_hive_orc_multifile_vectorized ORDER BY t1;
