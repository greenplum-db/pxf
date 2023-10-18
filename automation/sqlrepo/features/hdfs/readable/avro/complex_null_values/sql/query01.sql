-- @description query01 for PXF HDFS Readable Avro supported complex and null types test cases

SELECT * from avrotest_complex_null ORDER BY sourcetimestamp;
SELECT sourcetimestamp, meetmeprevious_view from avrotest_complex_null where meetmeprevious_view IS NULL ORDER BY sourcetimestamp;
SELECT sourcetimestamp, meetmeprevious_view from avrotest_complex_null where meetmeprevious_view IS NOT NULL ORDER BY sourcetimestamp;
