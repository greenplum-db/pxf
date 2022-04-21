-- @description query01 for PXF Hive ORC vectorized with repeating data cases
\pset null 'NIL'

\d pxf_hivevectorizedorc_repeating_no_nulls
SELECT * FROM pxf_hivevectorizedorc_repeating_no_nulls ORDER BY t1;

\d pxf_hivevectorizedorc_repeating_nulls
SELECT * FROM pxf_hivevectorizedorc_repeating_nulls ORDER BY t1;
