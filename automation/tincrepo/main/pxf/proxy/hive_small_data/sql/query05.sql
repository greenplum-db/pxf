-- @description query05 for PXF proxy test on small data, testuser is
-- prohibited to access the data, but because we are accessing data using a
-- server with no impersonation, the query fails. The service user is foobar
-- which does not have permissions to access the files either

-- start_matchsubs
--
-- m/You are now connected.*/
-- s/.*//g
--
-- m/.*inode=.*/
-- s/inode=.*?:-rwx/inode=SOME_PATH:-rwx/g
--
-- m/pxf:\/\/pxf_automation_data\/proxy\/([0-9a-zA-Z]).*\/data.txt/
-- s/pxf:\/\/pxf_automation_data\/proxy\/([0-9a-zA-Z]).*\/data.txt/pxf:\/\/pxf_automation_data\/proxy\/OTHER_USER\/data.txt/
--
-- m/DETAIL/
-- s/DETAIL/CONTEXT/
--
-- m/CONTEXT:.*line.*/
-- s/line \d* of //g
--
-- end_matchsubs

GRANT ALL ON TABLE pxf_proxy_hive_small_data_prohibited_no_impersonation TO PUBLIC;

-- both :USER and testuser use the same service user to access the data
SELECT * FROM pxf_proxy_hive_small_data_prohibited_no_impersonation ORDER BY name;

\set OLD_GP_USER :USER
DROP ROLE IF EXISTS testuser;
CREATE ROLE testuser LOGIN;

\connect - testuser
SELECT * FROM pxf_proxy_hive_small_data_prohibited_no_impersonation ORDER BY name;

\connect - :OLD_GP_USER
DROP ROLE IF EXISTS testuser;

