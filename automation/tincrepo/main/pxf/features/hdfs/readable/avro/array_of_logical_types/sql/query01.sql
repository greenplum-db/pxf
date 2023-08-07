-- @description query01 for PXF HDFS Readable Avro test case for Array of logical types.
set datestyle to 'ISO, MDY';
set timezone='utc';

\x

select * from array_of_logical_types;

set timezone='America/Los_Angeles';

select * from array_of_logical_types;
