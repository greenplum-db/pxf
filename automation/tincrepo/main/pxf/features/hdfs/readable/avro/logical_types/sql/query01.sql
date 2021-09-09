-- @description query01 for PXF HDFS Readable Avro test case for Avro logical types.

SELECT count(*) from avro_logical_types;

select * from avro_logical_types ;

SELECT CAST(timestampmillis AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'PDT' as timestampmillis, CAST(timestampmicros AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'PDT' as timestampmicros , localtimestampmicros, localtimestampmillis from avro_logical_types;

set timezone='America/Los_Angeles';

select * from avro_logical_types ;
