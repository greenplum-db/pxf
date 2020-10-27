DROP TABLE IF EXISTS orc_types;

CREATE TABLE orc_types (
id INT,
name STRING,
cdate DATE,
amt DOUBLE,
grade STRING,
b BOOLEAN,
tm TIMESTAMP,
bg BIGINT,
bin BINARY,
sml SMALLINT,
r FLOAT,
vc1 VARCHAR(5),
c1 CHAR(3),
dec1 decimal(38, 18),
dec2 decimal(5, 2),
dec3 decimal(13, 5),
num1 INT)
ROW FORMAT
DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/tmp/orc_types/csv';

DROP TABLE IF EXISTS hive_orc_all_types;

CREATE TABLE hive_orc_all_types (
id INT,
name STRING,
cdate DATE,
amt DOUBLE,
grade STRING,
b BOOLEAN,
tm TIMESTAMP,
bg BIGINT,
bin BINARY,
sml SMALLINT,
r FLOAT,
vc1 VARCHAR(5),
c1 CHAR(3),
dec1 decimal(38, 18),
dec2 decimal(5, 2),
dec3 decimal(13, 5),
num1 INT) STORED AS ORC;

INSERT INTO hive_orc_all_types
SELECT * FROM orc_types;

DROP TABLE IF EXISTS orc_types;
