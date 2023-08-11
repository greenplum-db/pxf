-- @description query01 for PXF multi user config driven test
SET datestyle TO 'ISO, MDY';

-- start_matchsubs
--
-- m/You are now connected.*/
-- s/.*//g
--
-- end_matchsubs

GRANT ALL ON TABLE pxf_jdbc_readable TO PUBLIC;
\set OLD_GP_USER :USER
DROP ROLE IF EXISTS testuser;
CREATE ROLE testuser LOGIN;

SELECT t1, t2, num1, dub1, dec1, tm, r, bg, b, tn, sml, dt, vc1, c1, encode(bin, 'escape') FROM pxf_jdbc_readable ORDER BY t1;

\connect - testuser
SET datestyle TO 'ISO, MDY';
SELECT t1, t2, num1, dub1, dec1, tm, r, bg, b, tn, sml, dt, vc1, c1, encode(bin, 'escape') FROM pxf_jdbc_readable ORDER BY t1;

\connect - :OLD_GP_USER
SET datestyle TO 'ISO, MDY';
SELECT t1, t2, num1, dub1, dec1, tm, r, bg, b, tn, sml, dt, vc1, c1, encode(bin, 'escape') FROM pxf_jdbc_readable ORDER BY t1;

DROP ROLE IF EXISTS testuser;
