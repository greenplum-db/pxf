-- @description query01 for PXF Multibyte delimiter, one byte delimiter option
SET datestyle TO 'ISO, MDY';

SELECT * from pxf_multibyte_onebyte_data ORDER BY n1;
