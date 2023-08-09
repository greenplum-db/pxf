-- @description query01 for PXF Multibyte delimiter, multi-character delim cases
SET datestyle TO 'ISO, MDY';

SELECT * from pxf_multibyte_multichar_data ORDER BY n1;

SELECT * from pxf_multibyte_multichar_data_with_skip ORDER BY n1;
