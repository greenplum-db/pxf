-- @description query01 for writing primitive ORC data types large dataset

SELECT count(*), sum(c_int) FROM pxf_orc_primitive_types_large_readable;
