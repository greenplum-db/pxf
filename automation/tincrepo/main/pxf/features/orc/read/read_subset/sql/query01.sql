-- @description query01 for ORC with Greenplum table as a subset of the ORC file

SELECT * FROM pxf_orc_primitive_types_subset ORDER BY name;

-- name, num1, vc1 are projected columns
SELECT num1, vc1 FROM pxf_orc_primitive_types_subset ORDER BY name;
