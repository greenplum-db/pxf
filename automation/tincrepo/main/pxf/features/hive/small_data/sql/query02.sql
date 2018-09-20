-- @description query02 query with complex WHERE clause and some projected columns

-- two queries with the same WHERE clause but different columns selected
-- number of rows should be the same
SELECT t1, t2 FROM pxf_hive_small_data WHERE to_timestamp(1505056530 + num1*86400)::DATE = '2017-09-15'::DATE AND t2 = 's_10';
SELECT t1, t2, num1 FROM pxf_hive_small_data WHERE to_timestamp(1505056530 + num1*86400)::DATE = '2017-09-15'::DATE AND t2 = 's_10';