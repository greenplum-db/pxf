-- @description query01 for PXF Multibyte delimiter, wrong profile case

-- start_matchsubs
--
-- m/\((.*):avro\)/
-- s/\((.*):avro\)/\(XXX:avro\)/
--
-- end_matchsubs
SELECT * from pxf_multibyte_wrong_profile ORDER BY age;