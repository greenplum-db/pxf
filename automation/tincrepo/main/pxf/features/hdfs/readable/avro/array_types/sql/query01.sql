-- @description query01 for PXF HDFS Readable Avro supported array types test cases

SELECT * from avrotest_arrays ORDER BY type_int, type_bytes desc, type_bytes_array desc;
SELECT * from avrotest_arrays_gpdb_arrays ORDER BY type_int, type_bytes desc, type_bytes_array desc;

SELECT type_int_array, type_double_array, type_string_array, type_float_array, type_long_array, type_bytes_array, type_boolean_array from avrotest_arrays ORDER BY type_int, type_bytes desc, type_bytes_array desc;
SELECT type_int_array, type_double_array, type_string_array, type_float_array, type_long_array, type_bytes_array, type_boolean_array from avrotest_arrays_gpdb_arrays ORDER BY type_int, type_bytes desc, type_bytes_array desc;