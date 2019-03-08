-- @description query02 for JDBC query with date by partitioning
SELECT t2, dec1, b::int, bin FROM pxf_jdbc_multiple_fragments_by_date WHERE num1 >= 5 ORDER BY t1;
