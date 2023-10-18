-- @description query02 for PXF proxy test on small data

-- start_matchsubs
--
-- m/You are now connected.*/
-- s/.*//g
--
-- end_matchsubs

GRANT ALL ON TABLE pxf_proxy_small_data_allowed TO PUBLIC;

\set OLD_GP_USER :USER
DROP ROLE IF EXISTS testuser;
CREATE ROLE testuser LOGIN;

\connect - testuser
SELECT name, num FROM pxf_proxy_small_data_allowed WHERE num > 50 ORDER BY name;

\connect - :OLD_GP_USER
DROP ROLE IF EXISTS testuser;