-- @description query01 for PXF Multibyte delimiter, 2-byte delim with quote and escape case
SET datestyle TO 'ISO, MDY';

SELECT * from pxf_multibyte_twobyte_withquote_withescape_data ORDER BY n1;
