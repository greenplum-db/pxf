DROP TABLE IF EXISTS orc_types;

CREATE TABLE orc_types (
b     BOOLEAN,
num1  INT,
tm    TIMESTAMP,
vc1   VARCHAR(5),
dec1  DECIMAL(38,18),
t1    STRING,
sml   SMALLINT)
ROW FORMAT
DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/tmp/orc_types/csv';

DROP TABLE IF EXISTS hive_orc_all_types;

CREATE TABLE hive_orc_all_types (
b     BOOLEAN,
num1  INT,
tm    TIMESTAMP,
vc1   VARCHAR(5),
dec1  DECIMAL(38,18),
t1    STRING,
sml   SMALLINT) STORED AS ORC;

INSERT INTO hive_orc_all_types
SELECT * FROM orc_types;

DROP TABLE IF EXISTS orc_types;
