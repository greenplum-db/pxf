-- @description query01 for PXF HDFS Readable Text Small Data cases reading multiple bzip2 compressed files

SELECT * from pxf_multibyte_twobyte_withbzip2_data ORDER BY name;