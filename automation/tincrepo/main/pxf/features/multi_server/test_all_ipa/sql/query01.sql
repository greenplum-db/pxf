-- @description query01 for PXF test for two secured clusters, a non-secure cluster, a cloud server, and an IPA server

SELECT * FROM pxf_multiserver_default ORDER BY name;

SELECT * FROM pxf_multiserver_secure_2 ORDER BY name;

SELECT * FROM pxf_multiserver_ipa ORDER BY name;

SELECT * FROM pxf_multiserver_non_secure ORDER BY name;

SELECT * FROM pxf_multiserver_s3 ORDER BY name;

SELECT * FROM pxf_multiserver_default UNION ALL
SELECT * FROM pxf_multiserver_secure_2 UNION ALL
SELECT * FROM pxf_multiserver_ipa UNION ALL
SELECT * FROM pxf_multiserver_non_secure UNION ALL
SELECT * FROM pxf_multiserver_s3
ORDER BY name;
