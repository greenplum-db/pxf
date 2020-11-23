-- data prep
-- start_ignore
{{ GPDB_REMOTE }}\!ssh {{ PGHOST }} mkdir -p {{ TEST_LOCATION }}
\!mkdir -p {{ TEST_LOCATION }}
COPY (
	SELECT format('row_%s',i::varchar(255)),
		i,
		i+0.0001,
		i*100000000000,
		CASE WHEN (i%2) = 0 THEN 'true' ELSE 'false' END
		from generate_series(1, 100) s(i)
	) TO '{{ TEST_LOCATION }}/data.csv'
	WITH (FORMAT 'csv');
{{ GPDB_REMOTE }}-- if GPDB is remote, will need to scp file down from there for beeline
{{ GPDB_REMOTE }}\!scp {{ PGHOST }}:{{ TEST_LOCATION }}/data.csv {{ TEST_LOCATION }}
\!{{ HCFS_CMD }} dfs -mkdir -p '{{ HCFS_SCHEME }}{{ HCFS_BUCKET }}{{ TEST_LOCATION }}'
\!{{ HCFS_CMD }} dfs -copyFromLocal '{{ TEST_LOCATION }}/data.csv' '{{ HCFS_SCHEME }}{{ HCFS_BUCKET }}{{ TEST_LOCATION }}'
-- end_ignore

-- FDW test
CREATE SERVER hdfs_smoke_test_hdfs_server
	FOREIGN DATA WRAPPER {{ HCFS_PROTOCOL }}_pxf_fdw
	OPTIONS (config '{{ SERVER_CONFIG }}');
CREATE USER MAPPING FOR CURRENT_USER SERVER hdfs_smoke_test_hdfs_server;
CREATE FOREIGN TABLE hdfs_smoke_test_foreign_table (
		name TEXT,
		num INTEGER,
		dub DOUBLE PRECISION,
		longNum BIGINT,
		bool BOOLEAN
	) SERVER hdfs_smoke_test_hdfs_server
	OPTIONS (resource '{{ HCFS_BUCKET }}{{ TEST_LOCATION }}/data.csv', format 'csv');
SELECT * FROM hdfs_smoke_test_foreign_table ORDER BY name;
SELECT name, num FROM hdfs_smoke_test_foreign_table WHERE num > 50 ORDER BY name;

-- start_ignore
{{ CLEAN_UP }}-- clean up HCFS and local disk
{{ CLEAN_UP }}\!{{ HCFS_CMD }} dfs -rm -r -f {{ HCFS_SCHEME }}{{ HCFS_BUCKET }}{{ TEST_LOCATION }}
{{ CLEAN_UP }}\!rm -rf {{ TEST_LOCATION }}
{{ CLEAN_UP }}{{ GPDB_REMOTE }}\!ssh {{ PGHOST }} rm -rf {{ TEST_LOCATION }}
-- end_ignore
