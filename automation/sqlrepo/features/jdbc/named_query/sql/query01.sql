-- @description query01 for JDBC named queries
-- start_ignore
-- end_ignore

SELECT gpdb_dept.name, count(*), max(gpdb_emp.salary)
FROM gpdb_dept JOIN gpdb_emp
ON gpdb_dept.id = gpdb_emp.dept_id
GROUP BY gpdb_dept.name;

SELECT * FROM pxf_jdbc_read_named_query ORDER BY name;

SELECT name, count FROM pxf_jdbc_read_named_query WHERE max > 100 ORDER BY name;

SELECT max(max) FROM pxf_jdbc_read_named_query;

SELECT * FROM pxf_jdbc_read_named_query_partitioned ORDER BY name;

SELECT name, count FROM pxf_jdbc_read_named_query_partitioned WHERE count > 2 ORDER BY name;