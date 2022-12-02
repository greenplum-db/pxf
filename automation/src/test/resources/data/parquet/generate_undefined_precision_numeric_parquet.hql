DROP TABLE IF EXISTS undefined_precision_numeric_csv;

CREATE EXTERNAL TABLE undefined_precision_numeric_csv
(description STRING, value DECIMAL(38,18))
ROW FORMAT
DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/tmp/csv/';

DROP TABLE IF EXISTS undefined_precision_numeric_parquet;

CREATE TABLE undefined_precision_numeric_parquet
(description STRING, value DECIMAL(38,18))
STORED AS PARQUET;

INSERT INTO undefined_precision_numeric_parquet
SELECT * FROM undefined_precision_numeric_csv;