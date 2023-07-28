-- @description query01 for PXF test for multiple servers

SELECT * FROM pxf_multiserver_default ORDER BY name;

SELECT * FROM pxf_multiserver_s3 ORDER BY name;

SELECT * FROM pxf_multiserver_default UNION ALL SELECT * FROM pxf_multiserver_s3 ORDER BY name;
