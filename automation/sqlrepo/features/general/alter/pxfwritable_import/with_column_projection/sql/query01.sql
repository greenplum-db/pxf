-- @description query01 test altering table, dropping columns and then adding them back

-- sets the date style and bytea output to the expected by the tests
SET datestyle='ISO, MDY';
SET bytea_output='escape';

SELECT s1, s2, n1, d1, dc1, CAST(tm AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'PDT' as tm, f, bg, b, tn, sml, vc1, c1, bin FROM pxf_alter_parquet_table ORDER BY s1;

-- Drop one of the columns
ALTER EXTERNAL TABLE pxf_alter_parquet_table DROP COLUMN tm;

-- Select query after alter
SELECT s1, s2, n1, d1, dc1, f, bg, b, tn, sml, vc1, c1, bin FROM pxf_alter_parquet_table ORDER BY s1;

-- Predicate push-down after alter
SELECT s1, s2, n1, d1, dc1, f, bg, b, tn, sml, vc1, c1, bin FROM pxf_alter_parquet_table WHERE s2 = 's_7' ORDER BY s1;

-- Column projection after alter
SELECT s1, d1 FROM pxf_alter_parquet_table ORDER BY s1;

-- Drop multiple columns
ALTER EXTERNAL TABLE pxf_alter_parquet_table
    DROP COLUMN n1,
    DROP COLUMN dc1,
    DROP COLUMN bg,
    DROP COLUMN vc1,
    DROP COLUMN bin;

-- Select query after alter
SELECT * FROM pxf_alter_parquet_table ORDER BY s1;

-- Predicate push-down after dropping multiple columns
SELECT * FROM pxf_alter_parquet_table WHERE sml = 20 ORDER BY s1;

-- Column projection after dropping multiple columns
SELECT f, tn FROM pxf_alter_parquet_table ORDER BY s1;

-- Add one of the columns back to the table
ALTER EXTERNAL TABLE pxf_alter_parquet_table ADD COLUMN n1 INT;

-- Select query after adding the column back
SELECT * FROM pxf_alter_parquet_table ORDER BY s1;

-- Predicate push-down after adding one column back
SELECT * FROM pxf_alter_parquet_table WHERE n1 <= 5 ORDER BY s1;

-- Column projection after adding one column back
SELECT s1, n1 FROM pxf_alter_parquet_table ORDER BY s1;

-- Edge cases
-- Drop the first column
ALTER EXTERNAL TABLE pxf_alter_parquet_table DROP COLUMN s1;

-- Select query after dropping the first column
SELECT * FROM pxf_alter_parquet_table WHERE s2 <> 's_16' ORDER BY s2;
