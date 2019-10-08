-- @description query01 for PXF with a Secured and Non-Secured Hive Small Data cases

SELECT *
from pxf_hive_small_data
ORDER BY t1;

SELECT *
from pxf_hive_small_data_hive_non_secure
ORDER BY t1;

SELECT *
from pxf_hive_small_data
UNION ALL
SELECT *
from pxf_hive_small_data_hive_non_secure
ORDER BY t1;
