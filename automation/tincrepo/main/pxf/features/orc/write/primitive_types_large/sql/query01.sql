-- @description query01 for writing primitive ORC data types large dataset

SELECT count(*), sum(col04) FROM orc_primitive_types_large_readable;
