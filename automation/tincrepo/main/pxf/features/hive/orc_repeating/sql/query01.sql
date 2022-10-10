-- @description query01 for PXF Hive ORC vectorized with repeating data cases
\pset null 'NIL'

SELECT * FROM pxf_hivevectorizedorc_repeating_no_nulls ORDER BY t1;

SELECT * FROM pxf_hivevectorizedorc_repeating_nulls ORDER BY t1;
