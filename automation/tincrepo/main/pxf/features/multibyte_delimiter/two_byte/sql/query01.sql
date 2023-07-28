-- @description query01 for PXF Multibyte delimiter, 2-byte delim cases

SELECT * from pxf_multibyte_twobyte_data ORDER BY n1;

SELECT * from pxf_multibyte_twobyte_data_with_skip ORDER BY n1;