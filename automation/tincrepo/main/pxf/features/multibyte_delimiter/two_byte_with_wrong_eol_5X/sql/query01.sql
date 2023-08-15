-- @description query01 for PXF Multibyte delimiter, 2-byte delim with wrong eol case

-- start_matchsubs
--
-- # create a match/subs

-- m/^WARNING/
-- s/WARNING/GP_IGNORE: WARNING/
--
-- end_matchsubs
SELECT * from pxf_multibyte_twobyte_wrong_eol_data ORDER BY n1;
