DROP TABLE IF EXISTS orc_types;

CREATE TABLE orc_types (
t1    STRING,
t2    STRING,
num1  INT,
dub1  DOUBLE,
dec1  DECIMAL(38,18),
tm    TIMESTAMP,
r     FLOAT,
bg    BIGINT,
b     BOOLEAN,
tn    TINYINT,
sml   SMALLINT,
dt    DATE,
vc1   VARCHAR(5),
c1    CHAR(3),
bin   BINARY)
ROW FORMAT
DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/tmp/orc_types/csv';

DROP TABLE IF EXISTS hive_orc_all_types;

CREATE TABLE hive_orc_all_types (
t1    STRING,
t2    STRING,
num1  INT,
dub1  DOUBLE,
dec1  DECIMAL(38,18),
tm    TIMESTAMP,
r     FLOAT,
bg    BIGINT,
b     BOOLEAN,
tn    TINYINT,
sml   SMALLINT,
dt    DATE,
vc1   VARCHAR(5),
c1    CHAR(3),
bin   BINARY) STORED AS ORC;

INSERT INTO hive_orc_all_types
SELECT * FROM orc_types;

DROP TABLE IF EXISTS orc_types;
