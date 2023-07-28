-- @description query02 for HCatalog aggregate query which has WHERE, GROUP BY, HAVING clauses
SELECT s1, SUM(n1) FROM hcatalog.default.hive_small_data
WHERE d1 >= 10
GROUP BY s1
HAVING SUM(n1) > 5
ORDER BY 2;