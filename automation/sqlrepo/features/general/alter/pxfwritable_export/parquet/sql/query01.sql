-- @description query01 test altering table, dropping columns during write

-- Insert all columns
INSERT INTO pxf_alter_write_parquet_table
SELECT s1, s2, n1, d1, dc1, tm, f, bg, b, tn, vc1, sml, c1, bin
FROM pxf_alter_parquet_primitive_types WHERE tn <= 10;

-- Drop two columns
ALTER EXTERNAL TABLE pxf_alter_write_parquet_table
    DROP COLUMN tm,
    DROP COLUMN bin;

-- Insert a subset of columns removing the dropped columns
INSERT INTO pxf_alter_write_parquet_table
SELECT s1, s2, n1, d1, dc1, f, bg, b, tn, vc1, sml, c1
FROM pxf_alter_parquet_primitive_types WHERE tn > 10;

-- Query the data with fewer columns
SELECT * FROM pxf_alter_write_parquet_table_r ORDER BY s1;
