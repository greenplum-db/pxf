-- @description query01 for primitive ORC data types. This test has two files:
-- the primitive ORC data file and a file that has a subset of columns in
-- different order
SET datestyle TO 'ISO, MDY';
SELECT * FROM pxf_orc_primitive_types_with_subset ORDER BY id;
