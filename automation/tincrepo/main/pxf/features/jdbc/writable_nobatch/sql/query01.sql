-- @description query for JDBC writable query
ALTER TABLE gpdb_types_nobatch_target ADD CONSTRAINT gpdb_types_nobatch_target_t1_key UNIQUE (t1);

INSERT INTO pxf_jdbc_writable_nobatch SELECT t1, t2, num1 FROM gpdb_types;

SELECT * FROM gpdb_types_nobatch_target ORDER BY t1;

-- start_matchsubs
--
-- # create a match/subs
--
-- m/\/gpdb\/v\d+\//
-- s/v\d+/SOME_VERSION/
--
-- m/file:.*;/
-- s/file:.*; lineNumber: \d+; columnNumber: \d+;/SOME_ERROR_LOCATION/g
--
-- m/DETAIL/
-- s/DETAIL/CONTEXT/
--
-- m/pxf:\/\/(.*)\/pxf_automation_data/
-- s/pxf:\/\/.*\/pxf_automation_data/pxf:\/\/pxf_automation_data/
--
-- m/CONTEXT:.*line.*/
-- s/line \d* of //g
--
-- end_matchsubs
INSERT INTO pxf_jdbc_writable_nobatch SELECT t1, t2, num1 FROM gpdb_types;
