{{ 5X_CREATE_EXTENSION }}
-- data prep
{{ GPDB_REMOTE }}\!ssh {{ PGHOST }} mkdir -p {{ TEST_LOCATION }}
\!mkdir -p {{ TEST_LOCATION }}
COPY (
	SELECT 'row_' || i::varchar(255),
		i,
		i+0.0001,
		i*100000000000,
		CASE WHEN (i%2) = 0 THEN 'true' ELSE 'false' END
		from generate_series(1, 100) s(i)
	) TO '{{ TEST_LOCATION }}/data.csv'
	WITH {{ POSTGRES_COPY_CSV }};
{{ GPDB_REMOTE }}-- if GPDB is remote, will need to scp file down from there for beeline
{{ GPDB_REMOTE }}\!scp {{ PGHOST }}:{{ TEST_LOCATION }}/data.csv {{ TEST_LOCATION }}
\!{{ HCFS_CMD }} dfs -mkdir -p {{ HCFS_SCHEME }}{{ HCFS_BUCKET }}{{ TEST_LOCATION }}
\!{{ HCFS_CMD }} dfs -copyFromLocal {{ TEST_LOCATION }}/data.csv {{ HCFS_SCHEME }}{{ HCFS_BUCKET }}{{ TEST_LOCATION }}

-- External Table test
CREATE EXTERNAL TABLE hdfs_smoke_test_external_table
	(name TEXT, num INTEGER, dub DOUBLE PRECISION, longNum BIGINT, bool BOOLEAN)
	LOCATION('pxf://{{ HCFS_BUCKET }}{{ TEST_LOCATION }}/data.csv?PROFILE={{ HCFS_PROTOCOL }}:csv{{ SERVER_PARAM }}')
	FORMAT 'CSV' (DELIMITER ',');

SELECT * FROM hdfs_smoke_test_external_table ORDER BY name;
SELECT name, num FROM hdfs_smoke_test_external_table WHERE num > 50 ORDER BY name;

{{ CLEAN_UP }}-- clean up HCFS and local disk
{{ CLEAN_UP }}\!{{ HCFS_CMD }} dfs -rm -r {{ HCFS_SCHEME }}{{ HCFS_BUCKET }}{{ TEST_LOCATION }}
{{ CLEAN_UP }}\!rm -rf {{ TEST_LOCATION }}
{{ CLEAN_UP }}{{ GPDB_REMOTE }}\!ssh {{ PGHOST }} rm -rf {{ TEST_LOCATION }}
