-- @description query01 for count lines multilined CSV file with header

SELECT COUNT(*) from pxf_file_as_row_with_header;

\x on
\pset format unaligned
SELECT * from pxf_file_as_row_with_header;