-- start_ignore
-- end_ignore
-- @description query01 for PXF test for cloud access where server is specified, no credentials are specified, and configuration file exists running alongside an HDFS setup
--

SELECT * FROM cloudaccess_server_no_credentials_valid_config_with_hdfs ORDER BY name;
