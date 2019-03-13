-- @description query01 for Parquet with Greenplum table as a subset of the parquet file
SELECT * FROM pxf_parquet_subset ORDER BY s1;

-- s1, d1, vc1 are projected columns
SELECT d1, vc1 FROM pxf_parquet_subset ORDER BY s1;
