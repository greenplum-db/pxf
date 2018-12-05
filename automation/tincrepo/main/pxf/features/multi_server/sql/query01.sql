-- @description query01 for PXF test for multiple servers

SELECT *  FROM pxf_multiserver_1 UNION ALL SELECT * FROM pxf_multiserver_2 ORDER BY name;
