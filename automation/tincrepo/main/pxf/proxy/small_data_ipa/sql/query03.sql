-- @description query03 for PXF proxy test on small data in IPA-based cluster

-- start_matchsubs
--
-- m/You are now connected.*/
-- s/.*//g
--
-- m/.*inode=.*/
-- s/inode=.*?:-rwx/inode=SOME_PATH:-rwx/g
--
-- m/pxf:\/\/(.*)\/pxf_automation_data\/[0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12}\/proxy\/([0-9a-zA-Z]).*\/data.txt/
-- s/pxf:\/\/(.*)\/pxf_automation_data\/[0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12}\/proxy\/([0-9a-zA-Z]).*\/data.txt/pxf:\/\/pxf_automation_data\/proxy\/OTHER_USER\/data.txt/
--
-- m/DETAIL/
-- s/DETAIL/CONTEXT/
--
-- m/CONTEXT:.*line.*/
-- s/line \d* of //g
--
-- end_matchsubs

GRANT ALL ON TABLE pxf_proxy_ipa_small_data_prohibited TO PUBLIC;

\set OLD_GP_USER :USER
DROP ROLE IF EXISTS testuser;
CREATE ROLE testuser LOGIN;

\connect - testuser
SELECT * FROM pxf_proxy_ipa_small_data_prohibited ORDER BY name;

\connect - :OLD_GP_USER
DROP ROLE IF EXISTS testuser;

