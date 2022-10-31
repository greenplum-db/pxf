-- @description query01 for Parquet Timestamp List data type. Timestamp stored in Parquet is UTC time. Greenplum would read it out in PST time
SET bytea_output=hex;

\pset null 'NIL'

SELECT * FROM pxf_parquet_timestamp_list_type ORDER BY id;