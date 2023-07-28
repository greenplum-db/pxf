-- start_ignore
-- end_ignore
-- @description query01 for PXF test for cloud write where server is specified, no credentials are specified, and configuration file exists running alongside an HDFS setup
--
-- start_matchsubs
--
-- # create a match/subs
--
-- m/WARNING.*cannot analyze this foreign table/
-- s/.*//
--
-- end_matchsubs
INSERT INTO cloudwrite_server_no_credentials_valid_config_with_hdfs_write SELECT md5(random()::text), round(random()*100) from generate_series(1,10);

SELECT count(*) FROM cloudaccess_server_no_credentials_valid_config_with_hdfs_write;
