-- @description query01 for PXF test for two secured clusters

SELECT *  FROM pxf_multiserver_default ORDER BY name;

SELECT * FROM pxf_multiserver_secure_2 ORDER BY name;

SELECT *  FROM pxf_multiserver_default UNION ALL SELECT * FROM pxf_multiserver_secure_2 ORDER BY name;
