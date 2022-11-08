DROP TABLE IF EXISTS parquet_types_csv;

CREATE EXTERNAL TABLE IF NOT EXISTS parquet_types_csv (
id       INT,
name     STRING,
cdate    DATE,
amt      DOUBLE,
grade    STRING,
b        BOOLEAN,
tm       TIMESTAMP,
bg       BIGINT,
bin      BINARY,
sml      SMALLINT,
r        FLOAT,
vc1      VARCHAR(5),
c1       CHAR(3),
dec1     decimal(38, 18),
dec2     decimal(5, 2),
dec3     decimal(13, 5),
num1     INT)
ROW FORMAT
DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/tmp/parquet_types/csv';

DROP TABLE IF EXISTS parquet_types;

CREATE TABLE parquet_types (
id       INT,
name     STRING,
cdate    DATE,
amt      DOUBLE,
grade    STRING,
b        BOOLEAN,
tm       TIMESTAMP,
bg       BIGINT,
bin      BINARY,
sml      SMALLINT,
r        FLOAT,
vc1      VARCHAR(5),
c1       CHAR(3),
dec1     decimal(38, 18),
dec2     decimal(5, 2),
dec3     decimal(13, 5),
num1     INT)
STORED AS PARQUET;

INSERT INTO parquet_types
SELECT * FROM parquet_types_csv;