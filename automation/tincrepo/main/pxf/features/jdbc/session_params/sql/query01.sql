-- @description query01 for JDBC query from view
SELECT * FROM pxf_jdbc_read_view_no_params WHERE name='client_min_messages' OR name='default_statistics_target' ORDER BY name;

SELECT * FROM pxf_jdbc_read_view_session_params WHERE name='client_min_messages' OR name='default_statistics_target' ORDER BY name;


