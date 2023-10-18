-- @description query01 for Operators that are not pushed down to PXF
--ANY
SELECT t1 FROM pxf_hive_orc_types WHERE num1 = ANY(array[9,10,11]);
SELECT t1 FROM pxf_hive_orc_types WHERE c1 = ANY(array['ab']);
SELECT t1 FROM pxf_hive_orc_types WHERE tm = ANY(array['2013-07-13 21:00:05'::timestamp, '2013-07-21 21:00:05'::timestamp]);
SELECT t1 FROM pxf_hive_orc_types WHERE r = ANY(array[11.7]::real[]);

--ALL
SELECT t1 FROM pxf_hive_orc_types WHERE num1 <= ALL(array[9,10,11]);
SELECT t1 FROM pxf_hive_orc_types WHERE c1 < ALL(array['abc', 'def']);
SELECT t1 FROM pxf_hive_orc_types WHERE tm < ALL(array['2013-07-23 21:00:05'::timestamp, '2013-07-21 21:00:05'::timestamp]);
SELECT t1 FROM pxf_hive_orc_types WHERE r < ALL(array[10.7, 11.7]::real[]);

--SOME
SELECT t1 FROM pxf_hive_orc_types WHERE num1 > SOME(array[9,10,11]);
SELECT t1 FROM pxf_hive_orc_types WHERE c1 < SOME(array['ab', 'abc']);
SELECT t1 FROM pxf_hive_orc_types WHERE tm < SOME(array['2013-07-13 21:00:05'::timestamp, '2013-07-21 21:00:05'::timestamp]);
SELECT t1 FROM pxf_hive_orc_types WHERE r > SOME(array[10.7, 11.7]::real[]);

--EXISTS
SELECT t1 FROM pxf_hive_orc_types WHERE EXISTS(SELECT t1 FROM pxf_hive_orc_types WHERE num1 = 10);
