-- @description query04 for PXF test on Multi Blocked data

SELECT cnt < 32768000 AS check FROM (SELECT COUNT(*) AS cnt FROM pxf_smoke_multi_blocked_data WHERE gp_segment_id = 0) AS a;