-- @description query01 for PXF HDFS Readable Json arrays as text test cases
\pset null 'NIL'
SELECT * FROM jsontest_array_as_text ORDER BY id;

SELECT id, (num_arr::json)->0 "num", (bool_arr::json)->1 "bool", (str_arr::json)->2 "str", (arr_arr::json)->0 "arr",
(obj_arr::json)->1 "obj", (obj::json)->'data'->'data'->'key' "key" FROM jsontest_array_as_text ORDER BY id;

SELECT * FROM jsontest_array_as_text_projections ORDER BY id;