-- @description query01 for count lines multilined CSV file with header

SELECT COUNT(*) from pxf_multi_csv_with_header;

\x on
\pset format unaligned
SELECT * from pxf_multi_csv_with_header;