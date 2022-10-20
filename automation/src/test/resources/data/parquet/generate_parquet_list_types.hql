!connect jdbc:hive2://localhost:10000/

DROP TABLE IF EXISTS parquet_list_types_tmp;
DROP TABLE IF EXISTS parquet_list_types_tmp;

CREATE TABLE parquet_list_types_tmp (
id INTEGER,
bool_arr             array<boolean>,
smallint_arr         array<tinyint>,
int_arr              array<int>,
bigint_arr           array<bigint>,
real_arr             array<float>,
double_arr           array<double>,
text_arr             array<string>,
bytea_arr            array<binary>,
char_arr             array<char(15)>,
varchar_arr          array<varchar(15)>,
date_arr             array<date>,
numeric_arr          array<decimal(38,18)>
)
ROW FORMAT
DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/tmp/parquet_types/csv_list';

DROP TABLE IF EXISTS foo;

CREATE TEMPORARY TABLE foo (a int);
INSERT INTO foo VALUES (1);

INSERT INTO parquet_list_types_tmp SELECT 1, array(boolean(null)), array(tinyint(50)), array(1), array(bigint(1)), array(float(null)), array(double(1.7e308)), array('this is a test string'), array(cast(null as binary)), array(cast('hello' as char(15))), array(cast('hello' as varchar(15))), array(cast('2022-10-07' as date)), array(cast(1.2345 as decimal(38,18))) FROM foo;

INSERT INTO parquet_list_types_tmp SELECT 2, array(false, true, boolean(1), boolean(0)), collect_list(tinyint(null)), array(2,3), array(bigint(null)), array(float(null)), array(double(1.0)), array('this is a string with "special" characters', 'this is a string without'), collect_list(binary(null)), array(cast('this is exactly' as char(15)), cast(' fifteen chars.' as char(15))), array(cast('this is exactly' as varchar(15)), cast(' fifteen chars.' as varchar(15))), array(cast('2022-10-07' as date)), array(cast(1.2345 as decimal(38,18))) FROM foo;

INSERT INTO parquet_list_types_tmp SELECT 3, array(true), array(tinyint(-128)), array(int(null)), collect_list(bigint(null)), array(float(-123456.987654321), float(9007199254740991)), array(double(5.678), double(9.10234)), array('hello', 'the next element is a string that says null', string(null)), array(unhex('DEADBEEF')), collect_list(cast(null as char(15))), array(cast(null as varchar(15))), array(cast('2022-10-07' as date), cast('2022-10-07' as date)), array(cast(1.2345 as decimal(38,18))) FROM foo;

INSERT INTO parquet_list_types_tmp SELECT 4, array(boolean(null)), array(tinyint(10), tinyint(20)), array(7, int(null), 8), array(bigint(-9223372036854775808), bigint(0)), array(float(2.3), float(4.5)), array(double(null)), array(string(null), string('')), array(binary(null), unhex('5c22')), array(cast(null as char(15))), array(cast(null as varchar(15))), array(cast('2022-10-07' as date), cast('2022-10-07' as date), cast(null as date)), array(cast(1.2345 as decimal(38,18))) FROM foo;

INSERT INTO parquet_list_types_tmp SELECT 5, array(true, false), array(tinyint(null)), collect_list(int(null)), array(bigint(null), bigint(9223372036854775807)), array(float(6.7), float(-8), float(null)), array(double(null)), array(string(null)), array(unhex('5C5C5C'), binary(null)), array(cast('specials \\ "' as char(15))), array(cast('specials \\ "' as varchar(15))), array(cast(null as date)), array(cast(1.2345 as decimal(38,18))) FROM foo;

INSERT INTO parquet_list_types_tmp SELECT 6, array(true, false, boolean(null)), array(tinyint(0), tinyint(127), tinyint(128)), array(int(2147483647), int(-2147483648)), array(bigint(1), bigint(null), bigint(300)), array(float(0.00000000000001)), array(double(null), double(8.431), double(-1.56)), array('this is a test string with \\ and "', string(null)), array(unhex('313233'), unhex('343536')), array(cast('test string' as char(15)), cast(null as char(15))), array(cast('test string' as varchar(15)), cast(null as varchar(15))), array(cast('2022-10-07' as date), cast('2012-01-01' as date)), array(cast(1.2345 as decimal(38,18))) FROM foo;

DROP TABLE IF EXISTS parquet_list_types;

CREATE TABLE parquet_list_types STORED AS PARQUET as SELECT * FROM parquet_list_types_tmp ORDER BY id;