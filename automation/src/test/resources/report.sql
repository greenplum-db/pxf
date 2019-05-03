SELECT gpdb_dept.name, count(), max(gpdb_emp.salary)
FROM gpdb_dept JOIN gpdb_emp
ON gpdb_dept.id = gpdb_emp.dept_id
GROUP BY gpdb_dept.name
