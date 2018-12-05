-- @description query01 for PXF test for multiple servers

SELECT *  FROM pxf_smoke_server1 UNION ALL SELECT * FROM pxf_smoke_server2 ORDER BY name;
