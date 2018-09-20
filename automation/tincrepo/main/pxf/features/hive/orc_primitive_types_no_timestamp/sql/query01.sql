-- @description query01 for HCatalog ORC all types cases
SELECT * FROM pxf_hive_orc_types ORDER BY t1;

--LIKE
SELECT num1 FROM pxf_hive_small_data WHERE t1 LIKE 'r%1%';
SELECT num1 FROM pxf_hive_small_data WHERE (t1 LIKE 'r%1' OR t2 LIKE 's_1%') AND dub1 < 12;

--IN
--text
SELECT * FROM pxf_hive_small_data WHERE t1 IN ('row1', 'row7');
--float8
SELECT t1 FROM pxf_hive_small_data WHERE dub1 IN (9,10,11);
--int4
SELECT * FROM pxf_hive_orc_types WHERE num1 IN (10, 11);
--numeric
SELECT * FROM pxf_hive_orc_types WHERE dec1 IN (1.23456,-1.23456);
--float4
SELECT * FROM pxf_hive_orc_types WHERE r IN ( CAST(8.7 as float4), CAST(9.7 as float4), CAST(10.7 as float4));
--int8
SELECT * FROM pxf_hive_orc_types WHERE bg IN (23456789);
--boolean
SELECT * FROM pxf_hive_orc_types WHERE b IN (TRUE);
SELECT * FROM pxf_hive_orc_types WHERE b IN (FALSE);
SELECT * FROM pxf_hive_orc_types WHERE b IN (FALSE, TRUE);
SELECT * FROM pxf_hive_orc_types WHERE b IN (TRUE, FALSE);
--int2
SELECT * FROM pxf_hive_orc_types WHERE tn IN (11,8);
--date
SELECT * FROM pxf_hive_orc_types WHERE dt IN ('2015-03-06');
--varchar
SELECT * FROM pxf_hive_orc_types WHERE vc1 IN ('abcd', 'abcde');
--char
SELECT * FROM pxf_hive_orc_types WHERE c1 IN ('abc');

--BETWEEN
SELECT t1 FROM pxf_hive_small_data WHERE dub1 BETWEEN 10 AND 15;

--IS NULL
SELECT t1 FROM pxf_hive_orc_types WHERE t2 IS NULL;
SELECT t1 FROM pxf_hive_orc_types WHERE dub1 IS NULL;
SELECT t1 FROM pxf_hive_orc_types WHERE b IS NULL;
SELECT t1 FROM pxf_hive_orc_types WHERE bin IS NULL;

--IS NOT NULL
SELECT t1 FROM pxf_hive_orc_types WHERE t2 IS NOT NULL;
SELECT t1 FROM pxf_hive_orc_types WHERE dub1 IS NOT NULL;
SELECT t1 FROM pxf_hive_orc_types WHERE b IS NOT NULL;
SELECT t1 FROM pxf_hive_orc_types WHERE bin IS NOT NULL;

--NOT
SELECT t1 FROM pxf_hive_small_data WHERE NOT (dub1 = 10);

--OR
SELECT t1 FROM pxf_hive_small_data WHERE dub1 = 10 OR num1 = 10;

--AND
SELECT t1 FROM pxf_hive_small_data WHERE dub1 = 12 AND num1 = 7;

--Complex logic expression, Var=Const, Const=Var, Const=Const, Var=Var
SELECT * FROM pxf_hive_small_data WHERE num1+5 = dub1;
SELECT * FROM pxf_hive_small_data WHERE num1+5 = dub1 AND 1=1;
SELECT * FROM pxf_hive_small_data WHERE num1 = 1 OR (t1 = 'row9' AND t2 = 's_14' AND num1 > 5);
SELECT * FROM pxf_hive_small_data WHERE num1 = 1 OR (t1 = 'row9' AND t2 = 's_14' AND num1 > 5 AND dub1 > 1) OR num1 = 2;

--Nested query
SELECT t_out.t1 FROM pxf_hive_small_data t_out WHERE t_out.num1 IN (SELECT t_in.dub1 FROM pxf_hive_small_data t_in WHERE t_in.dub1 <= 10);

--Join between PXF table and local GPDB table
SELECT local_t.t1, pxf_t.t2 FROM gpdb_small_data local_t, public.pxf_hive_small_data pxf_t WHERE pxf_t.t1 = local_t.t1 AND pxf_t.num1 > 6;

--Different primitive types as part of WHERE clause
--text column in WHERE clause
SELECT t1 FROM pxf_hive_orc_types WHERE t2 = 's_16';
SELECT t1 FROM pxf_hive_orc_types WHERE 's_16' = t2;

--integer column in WHERE clause
SELECT t1 FROM pxf_hive_orc_types WHERE num1 = 7;
SELECT t1 FROM pxf_hive_orc_types WHERE 7 = num1;

--double precision column in WHERE clause
SELECT t1 FROM pxf_hive_orc_types WHERE dub1 = 7;
SELECT t1 FROM pxf_hive_orc_types WHERE 7 = dub1;

--TODO: numeric column in WHERE clause, uncomment when HAWQ-1055 is done
--SELECT t1 FROM pxf_hive_orc_types WHERE dec1 = 7;

--real column in WHERE clause
SELECT t1 FROM pxf_hive_orc_types WHERE r = CAST(7.7 AS float4);
SELECT t1 FROM pxf_hive_orc_types WHERE CAST(7.7 AS float4) = r;

--bigint column in WHERE clause
SELECT t1 FROM pxf_hive_orc_types WHERE bg = 23456789;
SELECT t1 FROM pxf_hive_orc_types WHERE 23456789 = bg;

--boolean column in WHERE clause
SELECT t1 FROM pxf_hive_orc_types WHERE b IS TRUE;
SELECT t1 FROM pxf_hive_orc_types WHERE b IS FALSE;

--smallint column in WHERE clause
SELECT t1 FROM pxf_hive_orc_types WHERE tn = 7;
SELECT t1 FROM pxf_hive_orc_types WHERE 7 = tn;

--date column in WHERE clause
SELECT t1 FROM pxf_hive_orc_types WHERE dt = '2015-03-06';
SELECT t1 FROM pxf_hive_orc_types WHERE '2015-03-06' = dt;

--varchar(n) column in WHERE clause
SELECT t1 FROM pxf_hive_orc_types WHERE vc1 = 'abcd';
SELECT t1 FROM pxf_hive_orc_types WHERE 'abcd' = vc1;

--char(n) column in WHERE clause
SELECT t1 FROM pxf_hive_orc_types WHERE c1 = 'abc';
SELECT t1 FROM pxf_hive_orc_types WHERE 'abc' = c1;

--bytea column in WHERE clause
SELECT t1 FROM pxf_hive_orc_types WHERE bin = '1';
SELECT t1 FROM pxf_hive_orc_types WHERE '1' = bin;
