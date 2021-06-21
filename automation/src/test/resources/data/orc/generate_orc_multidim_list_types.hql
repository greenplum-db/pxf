DROP TABLE IF EXISTS orc_types;

CREATE TABLE orc_types (
id          INT,
bool_arr    ARRAY<ARRAY<BOOLEAN>>,
int2_arr    ARRAY<ARRAY<TINYINT>>,
int_arr     ARRAY<ARRAY<INT>>,
int8_arr    ARRAY<ARRAY<BIGINT>>,
float_arr   ARRAY<ARRAY<FLOAT>>,
float8_arr  ARRAY<ARRAY<DOUBLE>>,
text_arr    ARRAY<ARRAY<STRING>>,
bytea_arr   ARRAY<ARRAY<BINARY>>,
char_arr    ARRAY<ARRAY<CHAR(15)>>,
varchar_arr ARRAY<ARRAY<VARCHAR(15)>>)
ROW FORMAT
DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/tmp/orc_types/csv_list_multi';

CREATE TEMPORARY TABLE foo (a int);
INSERT INTO foo VALUES (1);

INSERT INTO orc_types SELECT 1, array(collect_list(boolean(null))), array(array(tinyint(50))), array(array(1)), array(array(bigint(1))), array(array(float(null))), array(array(double(1.7e308))), array(array('this is a test string')), array(array(cast(null as binary))), array(array(cast('hello' as char(15)))), array(array(cast('hello' as varchar(15)))) FROM foo;

INSERT INTO orc_types SELECT 2, array(array(false, true), array(boolean(1), boolean(0))), array(collect_list(tinyint(null))), array(array(2,3), array(int(null),int(null)), array(4,5)), array(array(bigint(null))), array(collect_list(float(null))), array(array(double(1.0))), array(array('this is a string with "special" characters'), array('this is a string without')), array(collect_list(binary(null))), array(array(cast('this is exactly' as char(15))), array(cast(' fifteen chars.' as char(15)))), array(array(cast('this is exactly' as varchar(15))), array(cast(' fifteen chars.' as varchar(15)))) FROM foo;

INSERT INTO orc_types SELECT 3, array(array(true)), array(array(tinyint(-128))), array(array(int(null))), array(collect_list(bigint(null))), array(array(float(-123456.987654321), float(9007199254740991))), array(array(double(5.678)), array(double(9.10234))), array(array('hello','world'), array('the next element is a string that says null', 'null')), array(array(unhex('DEADBEEF'))), array(collect_list(cast(null as char(15)))), array(collect_list(cast(null as varchar(15)))) FROM foo;

INSERT INTO orc_types SELECT 4, array(array(boolean(null))), array(array(tinyint(10), tinyint(20))), array(array(7, null), array(8, 9)), array(array(bigint(-9223372036854775808), bigint(0))), array(array(float(2.3)), array(float(4.5))), array(array(double(null))), array(collect_list(string(null))), array(array(binary(null), unhex('5c22'))), array(array(cast(null as char(15)))), array(array(cast(null as varchar(15)))) FROM foo;

INSERT INTO orc_types SELECT 5, array(array(true, false)), array(array(tinyint(null))), array(collect_list(int(null))), array(array(bigint(null),12), array(bigint(9223372036854775807),bigint(-1234567890))), array(array(float(6.7), float(-8)), array(float(null), float(null))), array(collect_list(double(null))), array(array(string(null))), array(array(unhex('5C5C5C'), unhex('5B48495D')), array(binary(null),unhex('68656C6C6F'))), array(array(cast('specials \\ "' as char(15)), cast(null as char(15))), array(cast(null as char(15)), cast(null as char(15)))), array(array(cast('specials \\ "' as varchar(15)),cast(null as varchar(15))), array(cast(null as varchar(15)),cast(null as varchar(15)))) FROM foo;

INSERT INTO orc_types SELECT 6, array(array(true, false), array(boolean(null), boolean(null))), array(array(tinyint(0), tinyint(127)), array(tinyint(null), tinyint(-126))), array(array(int(2147483647), int(-2147483648))), array(array(bigint(1), null), array(null, bigint(300))), array(array(float(0.00000000000001))), array(array(null, double(8.431)), array(double(-1.56), double(0.001))), array(array('this is a test string with \\ and "', null)), array(array(unhex('313233')), array(unhex('343536'))), array(array(cast('test string' as char(15)), cast(null as char(15))), array(cast('2 whitespace  ' as char(15)), cast('no whitespace' as char(15)))), array(array(cast('test string' as varchar(15)), cast(null as varchar(15))), array(cast('2 whitespace  ' as varchar(15)), cast('no whitespace' as varchar(15)))) FROM foo;

INSERT OVERWRITE local directory './orc_multidim_list_types' row format delimited fields terminated by ',' SELECT * FROM orc_types;

DROP TABLE IF EXISTS hive_orc_all_types;

CREATE TABLE hive_orc_all_types STORED AS ORC as SELECT * FROM orc_types ORDER BY id;

DROP TABLE IF EXISTS orc_types;
