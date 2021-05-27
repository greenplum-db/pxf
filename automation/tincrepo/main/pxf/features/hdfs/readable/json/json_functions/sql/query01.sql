-- @description query01 for PXF HDFS Readable Json Functions test cases
\pset null 'NIL'

-- This only works in Greenplum 6 since Greenplum 5 does not have json_array_elements_text() function
SELECT id,
       ARRAY(SELECT json_array_elements_text(num_arr::json))::decimal[]  AS numbers,
       ARRAY(SELECT json_array_elements_text(bool_arr::json))::boolean[] AS booleans,
       ARRAY(SELECT json_array_elements_text(str_arr::json))::text[]     AS strings,
       ARRAY(SELECT json_array_elements_text(arr_arr::json))::text[]     AS arrays
FROM jsontest_array_as_text
ORDER BY id;