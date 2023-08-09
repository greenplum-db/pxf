-- @description query01 for PXF Multibyte delimiter, 2-byte delim with CRLF case
SET datestyle TO 'ISO, MDY';

SELECT * from pxf_multibyte_twobyte_withcrlf_data ORDER BY n1;
