-- @description query01 for PXF multi user ddl and config driven test

-- start_matchsubs
--
-- m/You are now connected.*/
-- s/.*//g
--
-- end_matchsubs
GRANT ALL ON TABLE pxf_jdbc_readable_overrideddl TO PUBLIC;
\set OLD_GP_USER :USER
DROP ROLE IF EXISTS testuser;
CREATE ROLE testuser LOGIN;

\connect - testuser
SET timezone='America/Los_Angeles';
SELECT * FROM pxf_jdbc_readable_overrideddl ORDER BY t1;

\connect - :OLD_GP_USER
SET timezone='America/Los_Angeles';
SELECT * FROM pxf_jdbc_readable_overrideddl ORDER BY t1;

DROP ROLE IF EXISTS testuser;
