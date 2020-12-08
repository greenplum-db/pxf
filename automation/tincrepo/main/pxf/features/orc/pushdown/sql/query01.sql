-- @description query01 for ORC pushdown

-- no filter
select * from pxf_orc_primitive_types;

-- filter by integer
select * from pxf_orc_primitive_types where num1 = 11;

select * from pxf_orc_primitive_types where num1 < 11;

select * from pxf_orc_primitive_types where num1 > 11;

select * from pxf_orc_primitive_types where num1 <= 11;

select * from pxf_orc_primitive_types where num1 >= 11;

select * from pxf_orc_primitive_types where num1 <> 11;

select * from pxf_orc_primitive_types where num1 is null;

select * from pxf_orc_primitive_types where num1 is not null;

-- filter by bigint
select * from pxf_orc_primitive_types where bg = 2147483655;

select * from pxf_orc_primitive_types where bg < 0;

select * from pxf_orc_primitive_types where bg > 2147483655;

select * from pxf_orc_primitive_types where bg <= -2147483643;

select * from pxf_orc_primitive_types where bg >= 2147483655;

select * from pxf_orc_primitive_types where bg <> -1;

select * from pxf_orc_primitive_types where bg is null;

select * from pxf_orc_primitive_types where bg is not null;

-- filter by real
select * from pxf_orc_primitive_types where r = 8.7::real;

select * from pxf_orc_primitive_types where r < 8.7::real;

select * from pxf_orc_primitive_types where r > 8.7::real;

select * from pxf_orc_primitive_types where r <= 8.7::real;

select * from pxf_orc_primitive_types where r >= 8.7::real;

select * from pxf_orc_primitive_types where r <> 8.7::real;

select * from pxf_orc_primitive_types where r is null;

select * from pxf_orc_primitive_types where r is not null;

-- filter by text
select * from pxf_orc_primitive_types where grade = 'excellent';

select * from pxf_orc_primitive_types where grade < 'excellent';

select * from pxf_orc_primitive_types where grade > 'excellent';

select * from pxf_orc_primitive_types where grade <= 'excellent';

select * from pxf_orc_primitive_types where grade >= 'excellent';

select * from pxf_orc_primitive_types where grade <> 'excellent';

select * from pxf_orc_primitive_types where grade is null;

select * from pxf_orc_primitive_types where grade is not null;

-- filter by varchar
select * from pxf_orc_primitive_types where vc1 = 's_16';

select * from pxf_orc_primitive_types where vc1 < 's_10';

select * from pxf_orc_primitive_types where vc1 > 's_168';

select * from pxf_orc_primitive_types where vc1 <= 's_10';

select * from pxf_orc_primitive_types where vc1 >= 's_168';

select * from pxf_orc_primitive_types where vc1 <> 's_16';

select * from pxf_orc_primitive_types where vc1 IS NULL;

select * from pxf_orc_primitive_types where vc1 IS NOT NULL;

-- filter by char
select * from pxf_orc_primitive_types where c1 = 'EUR';

select * from pxf_orc_primitive_types where c1 < 'USD';

select * from pxf_orc_primitive_types where c1 > 'EUR';

select * from pxf_orc_primitive_types where c1 <= 'EUR';

select * from pxf_orc_primitive_types where c1 >= 'USD';

select * from pxf_orc_primitive_types where c1 <> 'USD';

select * from pxf_orc_primitive_types where c1 IS NULL;

select * from pxf_orc_primitive_types where c1 IS NOT NULL;

-- filter by smallint
select * from pxf_orc_primitive_types where sml = 1000;

select * from pxf_orc_primitive_types where sml < -1000;

select * from pxf_orc_primitive_types where sml > 31000;

select * from pxf_orc_primitive_types where sml <= 0;

select * from pxf_orc_primitive_types where sml >= 0;

select * from pxf_orc_primitive_types where sml <> 0;

select * from pxf_orc_primitive_types where sml IS NULL;

select * from pxf_orc_primitive_types where sml IS NOT NULL;

-- filter by date
select * from pxf_orc_primitive_types where cdate = '2019-12-04';

select * from pxf_orc_primitive_types where cdate < '2019-12-04';

select * from pxf_orc_primitive_types where cdate > '2019-12-20';

select * from pxf_orc_primitive_types where cdate <= '2019-12-06';

select * from pxf_orc_primitive_types where cdate >= '2019-12-15';

select * from pxf_orc_primitive_types where cdate <> '2019-12-15';

select * from pxf_orc_primitive_types where cdate IS NULL;

select * from pxf_orc_primitive_types where cdate IS NOT NULL;

-- filter by float8
select * from pxf_orc_primitive_types where amt = 1200;

select * from pxf_orc_primitive_types where amt < 1500;

select * from pxf_orc_primitive_types where amt > 2500;

select * from pxf_orc_primitive_types where amt <= 1500;

select * from pxf_orc_primitive_types where amt >= 2550;

select * from pxf_orc_primitive_types where amt <> 1200;

select * from pxf_orc_primitive_types where amt IS NULL;

select * from pxf_orc_primitive_types where amt IS NOT NULL;

-- filter by bytea
select * from pxf_orc_primitive_types where bin = '1';

select * from pxf_orc_primitive_types where bin < '1';

select * from pxf_orc_primitive_types where bin > '1';

select * from pxf_orc_primitive_types where bin <= '1';

select * from pxf_orc_primitive_types where bin >= '1';

select * from pxf_orc_primitive_types where bin <> '1';

select * from pxf_orc_primitive_types where bin IS NULL;

select * from pxf_orc_primitive_types where bin IS NOT NULL;

-- filter by id column with projection
select id from pxf_orc_primitive_types where id = 5;

select name, cdate, amt, sml, num1 from pxf_orc_primitive_types where id = 8 or (id > 10 and grade = 'bad');

select bin, bg, tm from pxf_orc_primitive_types where id = 15;

-- filter by date and amt
select * from pxf_orc_primitive_types where cdate > '2019-12-02' and cdate < '2019-12-12' and amt > 1500;

-- filter by date with or and amt
select * from pxf_orc_primitive_types where cdate > '2019-12-19' OR ( cdate <= '2019-12-15' and amt > 2000);

-- filter by date with or and amt using column projection
select id, amt, b from pxf_orc_primitive_types where cdate > '2019-12-19' OR ( cdate <= '2019-12-15' and amt > 2000);

-- filter by date or amt
select * from pxf_orc_primitive_types where cdate > '2019-12-20' OR amt < 1500;

-- filter by timestamp (not pushed)
select * from pxf_orc_primitive_types where tm = '2013-07-23 21:00:00';

-- filter by decimal (not pushed)
select * from pxf_orc_primitive_types where dec2 = 0;

-- filter by in (not pushed)
select * from pxf_orc_primitive_types where num1 in (11, 12);