SELECT dept.name, count(), max(emp.salary)
FROM dept JOIN emp
ON dept.id = emp.dept_id
WHERE dept.id < 10
GROUP BY dept.name