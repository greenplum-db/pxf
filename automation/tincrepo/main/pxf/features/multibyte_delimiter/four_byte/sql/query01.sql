-- @description query01 for PXF Multibyte delimiter, 4-byte delim cases

SELECT * from pxf_multibyte_fourbyte_data ORDER BY n1;

SELECT * from pxf_multibyte_fourbyte_data_with_skip ORDER BY n1;