-- @description query01 for reading strings contain NUL-byte from ORC files

SELECT * FROM pxf_orc_null_in_string ORDER BY id;
