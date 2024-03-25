CREATE EXTERNAL TABLE pxf_sample_1m  (a text, b integer, c boolean, d numeric) LOCATION ('pxf://dummy?PROFILE=system:sample&ROWS=1000000') FORMAT 'CSV';
CREATE EXTERNAL TABLE pxf_sample_10k (a text, b integer, c boolean, d numeric) LOCATION ('pxf://dummy?PROFILE=system:sample&ROWS=10000')   FORMAT 'CSV';
CREATE EXTERNAL TABLE pxf_sample_1k_err_random (a text, b integer, c boolean, d numeric) LOCATION ('pxf://dummy?PROFILE=system:sample&ROWS=1000&ERROR_SEGMENT=1') FORMAT 'CSV';
