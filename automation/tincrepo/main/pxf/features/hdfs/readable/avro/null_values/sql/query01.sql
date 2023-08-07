-- @description query01 for PXF HDFS Readable Avro supported complex types test cases

SELECT type_long, type_string, type_array, type_union, type_record, type_enum, type_fixed from avrotest_null where type_union IS NULL ORDER BY type_long;
SELECT type_long, type_string, type_array, type_union, type_record, type_enum, type_fixed from avrotest_null where type_union IS NOT NULL ORDER BY type_long;
