-- @description query03 for JDBC query with subset of columns in different order than the source table
SELECT "n@m2", "num 1" FROM pxf_jdbc_subset_of_fields_diff_order ORDER BY "num 1";
