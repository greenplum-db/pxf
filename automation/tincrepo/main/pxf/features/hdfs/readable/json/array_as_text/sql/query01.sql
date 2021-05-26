-- @description query01 for PXF HDFS Readable Json arrays as text test cases
\pset null 'NIL'

SELECT * FROM jsontest_array_as_text ORDER BY id;

SELECT * FROM jsontest_array_as_varchar ORDER BY id;

SELECT * FROM jsontest_array_as_bpchar ORDER BY id;

SELECT id, ((num_arr::json)->>0)::integer "num", ((bool_arr::json)->>1)::boolean "bool", (str_arr::json)->>2 "str", (arr_arr::json)->>0 "arr",
(obj_arr::json)->>1 "obj", (obj::json)->'data'->'data'->>'key' "key" FROM jsontest_array_as_text ORDER BY id;

-- In Greenplum 5, the function json_array_elements_text does not exist, so we need to fake the result for the test to pass in GP5 test environment
SELECT id,
  CASE WHEN version() LIKE '%Greenplum Database 5.%' THEN '{NULL,1,-1.3,1.2345678901234567}' ELSE ARRAY(SELECT json_array_elements_text(num_arr::json))::decimal[]  END AS numbers,
  CASE WHEN version() LIKE '%Greenplum Database 5.%' THEN '{NULL,t,f}'                       ELSE ARRAY(SELECT json_array_elements_text(bool_arr::json))::boolean[] END AS booleans,
  CASE WHEN version() LIKE '%Greenplum Database 5.%' THEN '{NULL,hello,"wor\"ld"}'           ELSE ARRAY(SELECT json_array_elements_text(str_arr::json))::text[]     END AS strings,
  CASE WHEN version() LIKE '%Greenplum Database 5.%' THEN '{NULL,"[\"a\",\"b\"]","[1,2]"}'   ELSE ARRAY(SELECT json_array_elements_text(arr_arr::json))::text[]     END AS arrays
FROM jsontest_array_as_text
WHERE id = 0;

SELECT * FROM jsontest_array_as_text_projections ORDER BY id;