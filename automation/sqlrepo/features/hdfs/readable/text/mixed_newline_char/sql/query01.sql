-- @description query01 for PXF HDFS Readable newline character

-- Display on for output consistency between GPDB 5 and 6
\x on
\pset format unaligned
SELECT * from pxf_newline_char;