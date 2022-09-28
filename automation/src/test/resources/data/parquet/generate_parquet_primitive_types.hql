!connect jdbc:hive2://localhost:10000/

DROP TABLE IF EXISTS parquet_primitive_types;
DROP TABLE IF EXISTS parquet_primitive_types;

CREATE TABLE parquet_primitive_types (
s1    STRING,
s2    STRING,
n1    INT,
d1    DOUBLE,
dc1   DECIMAL,
tm    TIMESTAMP,
f     FLOAT,
bg    BIGINT,
b     BOOLEAN,
tn    SMALLINT,
vc1   VARCHAR(5),
sml   SMALLINT,
c1    CHAR(3),
bin   BINARY)
ROW FORMAT
DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA INPATH '/tmp/parquet_primitive_types/csv' INTO TABLE parquet_primitive_types;

DROP TABLE IF EXISTS hive_parquet_primitive_types;

CREATE TABLE hive_parquet_primitive_types (
s1    STRING,
s2    STRING,
n1    INT,
d1    DOUBLE,
dc1   DECIMAL,
tm    TIMESTAMP,
f     FLOAT,
bg    BIGINT,
b     BOOLEAN,
tn    SMALLINT,
vc1   VARCHAR(5),
sml   SMALLINT,
c1    CHAR(3),
bin   BINARY) STORED AS PARQUET;

INSERT INTO hive_parquet_primitive_types SELECT * FROM parquet_primitive_types;

DROP TABLE IF EXISTS parquet_primitive_types;
