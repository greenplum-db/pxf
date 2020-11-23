-- FDW test
CREATE SERVER writable_smoke_test_hdfs
	FOREIGN DATA WRAPPER {{ HCFS_PROTOCOL }}_pxf_fdw
	OPTIONS (config '{{ SERVER_CONFIG }}');
CREATE USER MAPPING FOR CURRENT_USER SERVER writable_smoke_test_hdfs;
CREATE FOREIGN TABLE writable_smoke_test_foreign_table (
		name TEXT,
		num INTEGER,
		dub DOUBLE PRECISION,
		longNum BIGINT,
		bool BOOLEAN
	) SERVER writable_smoke_test_hdfs
	OPTIONS (resource '{{ HCFS_BUCKET }}{{ TEST_LOCATION }}/bzip_fdw', format 'csv', delimiter '|');

-- write to writable table
INSERT INTO writable_smoke_test_foreign_table
	SELECT format('row_%s',i::varchar(255)),
		i,
		i+0.0001,
		i*100000000000,
		CASE WHEN (i%2) = 0 THEN 'true'::boolean ELSE 'false'::boolean END
		from generate_series(1, 100) s(i);

-- Verify data entered HCFS correctly, no distributed by in FDW yet
\!{ for i in $({{ HCFS_CMD }} dfs -ls {{ HCFS_SCHEME }}{{ HCFS_BUCKET }}{{ TEST_LOCATION }}/bzip_fdw 2>/dev/null | tail -n +2 | awk '{print $NF}'); do {{ HCFS_CMD }} dfs -cat $i 2>/dev/null | head -1; done } | sort | head -1

SELECT * FROM writable_smoke_test_foreign_table ORDER BY name;
SELECT name, num FROM writable_smoke_test_foreign_table WHERE num > 50 ORDER BY name;

-- start_ignore
{{ CLEAN_UP }}-- clean up HCFS
{{ CLEAN_UP }}\!{{ HCFS_CMD }} dfs -rm -r -f {{ HCFS_SCHEME }}{{ HCFS_BUCKET }}{{ TEST_LOCATION }}
-- end_ignore
