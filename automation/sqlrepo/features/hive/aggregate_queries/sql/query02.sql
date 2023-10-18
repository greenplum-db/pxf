-- @description query02 for HCatalog aggregate query which has WHERE, GROUP BY, HAVING clauses
SELECT t1, SUM(num1) FROM pxf_hive_small_data
WHERE dub1 >= 10
GROUP BY t1
HAVING SUM(num1) > 5 AND SUM(num1) <= 10
ORDER BY 2;