{{ 5X_CREATE_EXTENSION }}
-- create writable external table
CREATE WRITABLE EXTERNAL TABLE writable_smoke_test_external_writable_table
	(name TEXT, num INTEGER, dub DOUBLE PRECISION, longNum BIGINT, bool BOOLEAN)
	LOCATION('pxf://{{ HCFS_BUCKET }}{{ TEST_LOCATION }}/bzip_et?PROFILE={{ HCFS_PROTOCOL }}:text{{ SERVER_PARAM }}')
	FORMAT 'TEXT' (DELIMITER '|')
	DISTRIBUTED BY (num);

-- write to writable table
INSERT INTO writable_smoke_test_external_writable_table
	SELECT 'row_' || i::varchar(255),
		i,
		i+0.0001,
		i*100000000000,
		CASE WHEN (i%2) = 0 THEN 'true'::boolean ELSE 'false'::boolean END
		from generate_series(1, 100) s(i);

-- Verify data entered HCFS correctly
\!{{ HCFS_CMD }} dfs -cat '{{ HCFS_SCHEME }}{{ HCFS_BUCKET }}{{ TEST_LOCATION }}/bzip_et/*' 2>/dev/null | sort -d

-- External Table test
CREATE EXTERNAL TABLE writable_smoke_test_external_readable_table
	(name TEXT, num INTEGER, dub DOUBLE PRECISION, longNum BIGINT, bool BOOLEAN)
	LOCATION('pxf://{{ HCFS_BUCKET }}{{ TEST_LOCATION }}/bzip_et?PROFILE={{ HCFS_PROTOCOL }}:text{{ SERVER_PARAM }}')
	FORMAT 'CSV' (DELIMITER '|');

SELECT * FROM writable_smoke_test_external_readable_table ORDER BY name;
SELECT name, num FROM writable_smoke_test_external_readable_table WHERE num > 50 ORDER BY name;

{{ CLEAN_UP }}-- clean up HCFS
{{ CLEAN_UP }}\!{{ HCFS_CMD }} dfs -rm -r {{ HCFS_SCHEME }}{{ HCFS_BUCKET }}{{ TEST_LOCATION }}
