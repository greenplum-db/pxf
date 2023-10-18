-- @description query03 for HCatalog test on small data on behalf of non-superuser
SET client_min_messages=WARNING;
DROP ROLE IF EXISTS zombie;
CREATE ROLE zombie;
SET ROLE=zombie;
SELECT s1, n1 FROM hcatalog.default.hive_table WHERE n1 > 50 ORDER BY s1;
SET client_min_messages=NOTICE;