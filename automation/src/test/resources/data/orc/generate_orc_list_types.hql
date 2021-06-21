DROP TABLE IF EXISTS orc_types;

CREATE TABLE orc_types (
id          INT,
bool_arr    ARRAY<BOOLEAN>,
int2_arr    ARRAY<TINYINT>,
int_arr     ARRAY<INT>,
int8_arr    ARRAY<BIGINT>,
float_arr   ARRAY<FLOAT>,
float8_arr  ARRAY<DOUBLE>,
text_arr    ARRAY<STRING>,
bytea_arr   ARRAY<BINARY>,
char_arr    ARRAY<CHAR(15)>,
varchar_arr ARRAY<VARCHAR(15)>)
ROW FORMAT
DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/tmp/orc_types/csv_list';

CREATE TEMPORARY TABLE foo (a int);
INSERT INTO foo VALUES (1);

INSERT INTO orc_types SELECT 1, collect_list(boolean(null)), array(tinyint(50)), array(1), array(bigint(1)), array(float(null)), array(double(1.7e308)), array('this is a test string'), array(cast(null as binary)), array(cast('hello' as char(15))), array(cast('hello' as varchar(15))) FROM foo;

INSERT INTO orc_types SELECT 2, array(false, true, boolean(1), boolean(0)), collect_list(tinyint(null)), array(2,3), array(bigint(null)), collect_list(float(null)), array(double(1.0)), array('this is a string with "special" characters', 'this is a string without'), collect_list(binary(null)), array(cast('this is exactly' as char(15)), cast(' fifteen chars.' as char(15))), array(cast('this is exactly' as varchar(15)), cast(' fifteen chars.' as varchar(15))) FROM foo;

INSERT INTO orc_types SELECT 3, array(true), array(tinyint(-128)), array(int(null)), collect_list(bigint(null)), array(float(-123456.987654321), float(9007199254740991)), array(double(5.678), double(9.10234)), array('hello', 'the next element is a string that says null', 'null'), array(unhex('DEADBEEF')), collect_list(cast(null as char(15))), collect_list(cast(null as varchar(15))) FROM foo;

INSERT INTO orc_types SELECT 4, array(boolean(null)), array(tinyint(10), tinyint(20)), array(7, null, 8), array(bigint(-9223372036854775808), bigint(0)), array(float(2.3), float(4.5)), array(double(null)), array(string(null), string('')), array(binary(null), unhex('5c22')), array(cast(null as char(15))), array(cast(null as varchar(15))) FROM foo;

INSERT INTO orc_types SELECT 5, array(true, false), array(tinyint(null)), collect_list(int(null)), array(bigint(null), bigint(9223372036854775807)), array(float(6.7), float(-8), null), collect_list(double(null)), array(string(null)), array(unhex('5C5C5C'), null), array(cast('specials \\ "' as char(15))), array(cast('specials \\ "' as varchar(15))) FROM foo;

INSERT INTO orc_types SELECT 6, array(true, false, null), array(tinyint(0), tinyint(127), tinyint(128)), array(int(2147483647), int(-2147483648)), array(bigint(1), null, bigint(300)), array(float(0.00000000000001)), array(null, double(8.431), double(-1.56)), array('this is a test string with \\ and "', null), array(unhex('313233'), unhex('343536')), array(cast('test string' as char(15)), cast(null as char(15))), array(cast('test string' as varchar(15)), cast(null as varchar(15))) FROM foo;

INSERT OVERWRITE local directory './orc_list_types' row format delimited fields terminated by ',' SELECT * FROM orc_types;

DROP TABLE IF EXISTS hive_orc_all_types;

CREATE TABLE hive_orc_all_types STORED AS ORC as SELECT * FROM orc_types ORDER BY id;

DROP TABLE IF EXISTS orc_types;
