-- @description query01 for PXF test for secured server and non-secured server working on the same PXF

SELECT * FROM pxf_multiserver_default ORDER BY name;

SELECT * FROM pxf_multiserver_non_secure ORDER BY name;

SELECT * FROM pxf_multiserver_default UNION ALL SELECT * FROM pxf_multiserver_non_secure ORDER BY name;
