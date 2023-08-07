-- @description query01 for PXF HDFS Readable Avro test case for Avro logical types.
set datestyle to 'ISO, MDY';

SELECT count(*) from avro_logical_types;

set timezone='utc';

select * from avro_logical_types;

set timezone='America/Los_Angeles';

select * from avro_logical_types;
