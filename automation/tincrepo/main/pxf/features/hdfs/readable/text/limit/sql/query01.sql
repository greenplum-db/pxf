-- @description query01 for PXF HDFS Readable test limit

SELECT COUNT(*) FROM (SELECT * FROM text_limit LIMIT 1000) AS a;

SELECT COUNT(*) FROM (SELECT * FROM text_limit) AS b;
