SELECT n1, count(n1) AS c, sum(tn) AS s
FROM jdbc_hive_types_table
WHERE vc1 = 'abcde'
GROUP BY n1
