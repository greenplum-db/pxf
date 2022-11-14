-- @description query01 for list ORC data types
\pset null 'NIL'
SET bytea_output=hex;


SELECT * FROM pxf_orc_multidim_list_types ORDER BY id;

SELECT id, bool_arr[1:1], int2_arr[2:2], int_arr[1:1], int8_arr[2:2], float_arr[1:1], float8_arr[1:1], text_arr[1:1], bytea_arr[1:1], char_arr[1:1], varchar_arr[1:1]  FROM pxf_orc_multidim_list_types ORDER BY id;

SELECT id, bool_arr[1][1], int2_arr[2][2], int_arr[1][1], int8_arr[2][2], float_arr[1][1], float8_arr[1][1], text_arr[1][1], bytea_arr[1][1], char_arr[1][1], varchar_arr[1][1] FROM pxf_orc_multidim_list_types ORDER BY id;

SET bytea_output=escape;

SELECT bytea_arr FROM pxf_orc_multidim_list_types ORDER BY id;