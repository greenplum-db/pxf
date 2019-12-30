CREATE EXTERNAL TABLE IF NOT EXISTS precision_numeric_csv (
description STRING,
a DECIMAL(5,  2),
b DECIMAL(12, 2),
c DECIMAL(18, 18),
d DECIMAL(24, 16),
e DECIMAL(30, 5),
f DECIMAL(34, 30),
g DECIMAL(38, 10),
h DECIMAL(38, 38))
ROW FORMAT
DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/tmp/csv/';

DROP TABLE IF EXISTS precision_numeric_parquet;

CREATE TABLE precision_numeric_parquet (
description STRING,
a DECIMAL(5,  2),
b DECIMAL(12, 2),
c DECIMAL(18, 18),
d DECIMAL(24, 16),
e DECIMAL(30, 5),
f DECIMAL(34, 30),
g DECIMAL(38, 10),
h DECIMAL(38, 38))
STORED AS PARQUET;

INSERT INTO precision_numeric_parquet
SELECT * FROM precision_numeric_csv;