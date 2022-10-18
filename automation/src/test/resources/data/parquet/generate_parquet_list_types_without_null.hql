!connect jdbc:hive2://localhost:10000/

DROP TABLE IF EXISTS parquet_list_types_without_null_tmp;
DROP TABLE IF EXISTS parquet_list_types_without_null_tmp;

CREATE TABLE parquet_list_types_without_null_tmp (
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
STORED AS PARQUET;

DROP TABLE IF EXISTS foo;

CREATE TEMPORARY TABLE foo (a int);
INSERT INTO foo VALUES (1);

INSERT INTO parquet_list_types_without_null_tmp SELECT 1, collect_list(true), array(tinyint(50)), array(1), array(bigint(1)), array(float(1.11)), array(double(1.7e308)), array('this is a test string'), array(unhex('DEADBEEF')), array(cast('hello' as char(15))), array(cast('hello' as varchar(15))), array(cast('2022-10-07' as date)), array(cast(1.23456 as decimal(38,18))) FROM foo;

INSERT INTO parquet_list_types_without_null_tmp SELECT 2, array(false, true, boolean(1), boolean(0)), array(tinyint(128),tinyint(96)), array(2,3), array(bigint(-9223372036854775808), bigint(223372036854775808)), array(float(-123456.987654321), float(123456.987654321)), array(double(1.0), double(-99.9)), array('this is a string with "special" characters', 'this is a string without'), array(unhex('DEADBEEF'), unhex('ADBEEF')), array(cast('this is exactly' as char(15)), cast(' fifteen chars.' as char(15))), array(cast('this is exactly' as varchar(15)), cast(' fifteen chars.' as varchar(15))), array(cast('2022-10-07' as date),cast('2022-10-08' as date)), array(cast(1.23456 as decimal(38,18)), cast(1.23456 as decimal(38,18))) FROM foo;

DROP TABLE IF EXISTS parquet_list_types_without_null;

CREATE TABLE parquet_list_types_without_null STORED AS PARQUET as SELECT * FROM parquet_list_types_without_null_tmp ORDER BY id;