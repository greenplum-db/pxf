-- @description query02 for PXF HDFSHA test on small data in IPA-based cluster

-- start_matchsubs
--
-- m/You are now connected.*/
-- s/.*//g
--
-- end_matchsubs

GRANT ALL ON TABLE pxf_hdfsha_hdfs_ipa TO PUBLIC;

\set OLD_GP_USER :USER
DROP ROLE IF EXISTS testuser;
CREATE ROLE testuser LOGIN;

\connect - testuser
SELECT * FROM pxf_hdfsha_hdfs_ipa ORDER BY name;

\connect - :OLD_GP_USER
DROP ROLE IF EXISTS testuser;