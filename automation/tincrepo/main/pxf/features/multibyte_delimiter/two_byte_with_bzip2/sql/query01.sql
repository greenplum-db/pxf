-- @description query01 for PXF Multibyte delimiter, 2-byte delim cases with compressed bzip2 file

SELECT * from pxf_multibyte_twobyte_withbzip2_data ORDER BY name;