-- @description query01 for primitive ORC data types
SET datestyle TO 'ISO, MDY';
SELECT * FROM pxf_orc_primitive_types ORDER BY id;
