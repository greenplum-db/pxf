-- @description query01 for writing Parquet Timestamp List data type. Timestamp stored in Parquet is UTC time. Extract every timestamp first then convert them into PDT time
SET bytea_output=hex;
SET datestyle TO 'ISO, MDY';

\pset null 'NIL'

CREATE OR REPLACE VIEW parquet_timestamp_list_breakdown_view AS
SELECT id, tm_arr[1] tm_arr_elem1, tm_arr[2] tm_arr_elem2, tm_arr[3] tm_arr_elem3 FROM pxf_parquet_read_timestamp_list;

CREATE OR REPLACE VIEW parquet_timestamp_list_PDT_view AS
SELECT id,
       CAST(tm_arr_elem1 AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'PDT' as tm_arr_elem1,
       CAST(tm_arr_elem2 AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'PDT' as tm_arr_elem2,
       CAST(tm_arr_elem3 AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'PDT' as tm_arr_elem3 FROM parquet_timestamp_list_breakdown_view;

SELECT * FROM parquet_timestamp_list_PDT_view ORDER BY id;
