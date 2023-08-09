-- @description query01 for PXF Multibyte delimiter, 3-byte delim cases
SET datestyle TO 'ISO, MDY';

SELECT * from pxf_multibyte_threebyte_data ORDER BY n1;

SELECT * from pxf_multibyte_threebyte_data_with_skip ORDER BY n1;
