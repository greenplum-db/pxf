-- start_ignore
-- end_ignore
-- @description query01 for PXF test for cloud access where server and credentials are specified, and configuration file is invalid running alongside an HDFS setup
--

SELECT * FROM cloudaccess_server_credentials_invalid_config_with_hdfs ORDER BY name;
