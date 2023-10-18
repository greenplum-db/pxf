-- @description query01 for pushing down predicates of type char that were
-- padded during write. Hive right trims values for type char, for example
-- when a value 'a  ' is inserted into a Hive table stored as parquet, Hive will
-- only store 'a' in the parquet file.

-- Display on for output consistency between GPDB 5 and 6
\x on
\pset format unaligned
SELECT s1, s2, n1, d1, dc1, f, bg, b, tn, sml, vc1, c1, bin FROM parquet_view WHERE c1 = 'a' OR c1 = e'b\t' OR c1 = e'c\n' ORDER BY s1;

SELECT s1, s2, n1, d1, dc1, f, bg, b, tn, sml, vc1, c1, bin FROM parquet_view WHERE c1 = 'a  ' ORDER BY s1;