-- @description query01 for PXF Multibyte delimiter, 2-byte delim with quote case
SET datestyle TO 'ISO, MDY';

SELECT * from pxf_multibyte_twobyte_withquote_data ORDER BY n1;
