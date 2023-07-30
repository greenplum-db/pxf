-- @description query01 for JDBC writable query
ALTER TABLE gpdb_types_target ADD CONSTRAINT gpdb_types_target_t1_key UNIQUE (t1);

-- FIXME: this insert fails if datestyle is 'Postgres, MDY'
SET datestyle TO 'ISO, MDY';
INSERT INTO pxf_jdbc_writable SELECT * FROM gpdb_types;

SELECT * FROM gpdb_types_target ORDER BY t1;

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
INSERT INTO pxf_jdbc_writable SELECT * FROM gpdb_types;
