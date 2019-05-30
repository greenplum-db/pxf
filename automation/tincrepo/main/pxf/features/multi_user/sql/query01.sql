-- @description query01 for PXF multi user config driven test

-- start_matchsubs
--
-- m/You are now connected.*/
-- s/.*//g
--
-- end_matchsubs

GRANT ALL ON TABLE pxf_jdbc_readable TO PUBLIC;
\set OLD_GP_USER :USER
DROP ROLE IF EXISTS testuser;
CREATE ROLE testuser LOGIN RESOURCE QUEUE pg_default;

SELECT * FROM pxf_jdbc_readable ORDER BY t1;

\connect - testuser
SELECT * FROM pxf_jdbc_readable ORDER BY t1;

\connect - :OLD_GP_USER
SELECT * FROM pxf_jdbc_readable ORDER BY t1;

DROP ROLE IF EXISTS testuser;