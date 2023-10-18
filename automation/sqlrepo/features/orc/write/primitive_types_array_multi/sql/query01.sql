-- @description query01 for writing arrays of multi-dimensional primitive ORC data types
-- start_matchsubs
--
-- # create a match/subs
--
-- m/DETAIL/
-- s/DETAIL/CONTEXT/
--
-- m/(.*)ERROR:/
-- s/(.*)ERROR:/ERROR:/
--
-- m/Error parsing array element: (.*) was not of expected type/
-- s/Error parsing array element: (.*) was not of expected type/Error parsing array element: val was not of expected type/
--
-- m/malformed boolean literal (.*)/
-- s/malformed boolean literal (.*)/malformed boolean literal value/
--
-- end_matchsubs

\pset null 'NIL'
SET bytea_output=hex;

INSERT INTO orc_primitive_arrays_multi_bool_writable SELECT id, bool_arr FROM orc_primitive_arrays_multi_heap;
INSERT INTO orc_primitive_arrays_multi_bytea_writable SELECT id, bytea_arr FROM orc_primitive_arrays_multi_heap;
INSERT INTO orc_primitive_arrays_multi_int_writable SELECT id, int_arr FROM orc_primitive_arrays_multi_heap;
INSERT INTO orc_primitive_arrays_multi_float_writable SELECT id, float_arr FROM orc_primitive_arrays_multi_heap;
INSERT INTO orc_primitive_arrays_multi_date_writable SELECT id, date_arr FROM orc_primitive_arrays_multi_heap;
INSERT INTO orc_primitive_arrays_multi_text_writable SELECT id, text_arr FROM orc_primitive_arrays_multi_heap;

SELECT * FROM orc_primitive_arrays_multi_text_readable ORDER BY id;