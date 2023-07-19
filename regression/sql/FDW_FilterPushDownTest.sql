
----------------------------------------------------------------------------------
------ Check that Filter Push Down is working and disabled in normal cases -------
----------------------------------------------------------------------------------

-- Check that the filter is being pushed down. We create an external table
-- that returns the filter being sent from the C-side
CREATE FOREIGN DATA WRAPPER pxf_filter_push_down_fdw
    HANDLER pxf_fdw_handler
    VALIDATOR pxf_fdw_validator
    OPTIONS ( protocol 'system:filter', mpp_execute 'all segments' );

CREATE SERVER pxf_filter_push_down_server
    FOREIGN DATA WRAPPER pxf_filter_push_down_fdw;

CREATE USER MAPPING FOR CURRENT_USER SERVER pxf_filter_push_down_server;

DROP FOREIGN TABLE IF EXISTS test_filter CASCADE;

CREATE FOREIGN TABLE test_filter (t0 text, a1 integer, b2 boolean, c3 numeric, d4 text, filterValue text)
    SERVER pxf_filter_push_down_server
    OPTIONS ( resource 'dummy_path', delimiter ',');

SET optimizer = off;

SELECT * FROM test_filter WHERE  t0 = 'A' and a1 = 0 ORDER BY t0, a1;

SELECT t0, a1, b2, filterValue FROM test_filter WHERE  t0 = 'B' AND a1 <= 1 ORDER BY t0, a1;

SELECT t0, a1, b2, filterValue FROM test_filter WHERE  t0 = 'B' AND (a1 = 1 OR a1 = 10) ORDER BY t0, a1;

SELECT t0, a1, b2, filterValue FROM test_filter WHERE  t0 = 'B' OR (a1 >= 0 AND a1 <= 2) ORDER BY t0, a1;

SELECT t0, a1, b2, filterValue FROM test_filter WHERE  b2 = false ORDER BY t0, a1;

SELECT t0, a1, filtervalue FROM test_filter WHERE a1 < 5 AND b2 = false ORDER BY t0, a1;

SELECT t0, a1, b2, filterValue FROM test_filter WHERE  b2 = false AND (a1 = 1 OR a1 = 10) ORDER BY t0, a1;

SELECT t0, a1, b2, filterValue FROM test_filter WHERE  b2 = false OR (a1 >= 0 AND a1 <= 2) ORDER BY t0, a1;

SET optimizer = on;

SELECT t0, a1, b2, filterValue FROM test_filter WHERE  t0 = 'A' and a1 = 0 ORDER BY t0, a1;

SELECT t0, a1, b2, filterValue FROM test_filter WHERE  t0 = 'B' AND a1 <= 1 ORDER BY t0, a1;

SELECT t0, a1, b2, filterValue FROM test_filter WHERE  t0 = 'B' AND (a1 = 1 OR a1 = 10) ORDER BY t0, a1;

SELECT t0, a1, b2, filterValue FROM test_filter WHERE  t0 = 'B' OR (a1 >= 0 AND a1 <= 2) ORDER BY t0, a1;

SELECT t0, a1, b2, filterValue FROM test_filter WHERE  b2 = false ORDER BY t0, a1;

SELECT t0, a1, filtervalue FROM test_filter WHERE a1 < 5 AND b2 = false ORDER BY t0, a1;

SELECT t0, a1, b2, filterValue FROM test_filter WHERE  b2 = false AND (a1 = 1 OR a1 = 10) ORDER BY t0, a1;

SELECT t0, a1, b2, filterValue FROM test_filter WHERE  b2 = false OR (a1 >= 0 AND a1 <= 2) ORDER BY t0, a1;

-- Now let's make sure nothing gets pushed down when we disable predicate
-- push-down by setting the disable_ppd option to true

ALTER SERVER pxf_filter_push_down_server
    OPTIONS ( ADD disable_ppd 'true' );

SELECT t0, a1, b2, filterValue FROM test_filter WHERE  t0 = 'A' and a1 = 0 ORDER BY t0, a1;

-- Drop the disable_ppd option to re-enable predicate push-down for the server

ALTER SERVER pxf_filter_push_down_server
    OPTIONS ( DROP disable_ppd );

-------------------------------------------------------------------------------------
------ Check that Filter Push Down is working for varchar, bpchar and numeric -------
-------------------------------------------------------------------------------------

-- Recreate the same table as above, but now we use varchar for the t0 column
-- type and 2 more columns c3 and c4 use numeric and bpchar. We want to make
-- sure varchar, bpchar and numeric predicates are being pushed down.

DROP FOREIGN TABLE IF EXISTS test_filter CASCADE;

CREATE FOREIGN TABLE test_filter (t0 varchar(1), a1 integer, b2 boolean, c3 numeric, d4 char(10), filterValue text)
    SERVER pxf_filter_push_down_server
    OPTIONS ( resource 'dummy_path', delimiter ',');

SET optimizer = off;

SELECT t0, a1, b2, filterValue FROM test_filter WHERE  t0 = 'A' and a1 = 0 ORDER BY t0, a1;

SELECT t0, a1, b2, filterValue FROM test_filter WHERE  t0 = 'B' AND a1 <= 1 ORDER BY t0, a1;

SELECT t0, a1, b2, filterValue FROM test_filter WHERE  t0 = 'B' AND (a1 = 1 OR a1 = 10) ORDER BY t0, a1;

SELECT t0, a1, b2, filterValue FROM test_filter WHERE  t0 = 'B' OR (a1 >= 0 AND a1 <= 2) ORDER BY t0, a1;

SELECT t0, a1, b2, filterValue FROM test_filter WHERE  b2 = false ORDER BY t0, a1;

SELECT t0, a1, filtervalue FROM test_filter WHERE a1 < 5 AND b2 = false ORDER BY t0, a1;

SELECT t0, a1, b2, filterValue FROM test_filter WHERE  b2 = false AND (a1 = 1 OR a1 = 10) ORDER BY t0, a1;

SELECT t0, a1, b2, filterValue FROM test_filter WHERE  b2 = false OR (a1 >= 0 AND a1 <= 2) ORDER BY t0, a1;

SELECT * FROM test_filter WHERE  c3 = 1.11 ORDER BY t0, a1;

SELECT * FROM test_filter WHERE  c3 > 1.11 ORDER BY t0, a1;

SELECT * FROM test_filter WHERE  c3 <> 1.11 ORDER BY t0, a1;

SELECT * FROM test_filter WHERE  d4 = 'AA' ORDER BY t0, a1;

SET optimizer = on;

SELECT t0, a1, b2, filterValue FROM test_filter WHERE  t0 = 'A' and a1 = 0 ORDER BY t0, a1;

SELECT t0, a1, b2, filterValue FROM test_filter WHERE  t0 = 'B' AND a1 <= 1 ORDER BY t0, a1;

SELECT t0, a1, b2, filterValue FROM test_filter WHERE  t0 = 'B' AND (a1 = 1 OR a1 = 10) ORDER BY t0, a1;

SELECT t0, a1, b2, filterValue FROM test_filter WHERE  t0 = 'B' OR (a1 >= 0 AND a1 <= 2) ORDER BY t0, a1;

SELECT t0, a1, b2, filterValue FROM test_filter WHERE  b2 = false ORDER BY t0, a1;

SELECT t0, a1, filtervalue FROM test_filter WHERE a1 < 5 AND b2 = false ORDER BY t0, a1;

SELECT t0, a1, b2, filterValue FROM test_filter WHERE  b2 = false AND (a1 = 1 OR a1 = 10) ORDER BY t0, a1;

SELECT t0, a1, b2, filterValue FROM test_filter WHERE  b2 = false OR (a1 >= 0 AND a1 <= 2) ORDER BY t0, a1;

SELECT * FROM test_filter WHERE  c3 = 1.11 ORDER BY t0, a1;

SELECT * FROM test_filter WHERE  c3 > 1.11 ORDER BY t0, a1;

SELECT * FROM test_filter WHERE  c3 <> 1.11 ORDER BY t0, a1;

SELECT * FROM test_filter WHERE  d4 = 'AA' ORDER BY t0, a1;

-- Now let's make sure nothing gets pushed down by setting the disable_ppd
-- option to false at the foreign table level instead of the server level

ALTER FOREIGN TABLE test_filter
    OPTIONS ( ADD disable_ppd 'true' );

SELECT t0, a1, b2, filterValue FROM test_filter WHERE  t0 = 'A' and a1 = 0 ORDER BY t0, a1;

-----------------------------------------------------------------------
------ Check that Filter Push Down is working with HEX delimiter ------
-----------------------------------------------------------------------

DROP FOREIGN TABLE IF EXISTS test_filter CASCADE;

CREATE FOREIGN TABLE test_filter (t0 text, a1 integer, b2 boolean, c3 numeric, d4 char(10), filterValue text)
    SERVER pxf_filter_push_down_server
    OPTIONS ( resource 'dummy_path', delimiter E'\x01');

SET optimizer = off;

SELECT t0, a1, b2, filterValue FROM test_filter WHERE t0 = 'J' and a1 = 9 ORDER BY t0, a1;

SET optimizer = on;

SELECT t0, a1, b2, filterValue FROM test_filter WHERE t0 = 'J' and a1 = 9 ORDER BY t0, a1;

-- Recreate the same table as above and make sure that varchar is also being
-- pushed down.

DROP FOREIGN TABLE IF EXISTS test_filter CASCADE;

CREATE FOREIGN TABLE test_filter (t0 varchar(1), a1 integer, b2 boolean, c3 numeric, d4 char(10), filterValue text)
    SERVER pxf_filter_push_down_server
    OPTIONS ( resource 'dummy_path', delimiter E'\x01');

SET optimizer = off;

SELECT t0, a1, b2, filterValue FROM test_filter WHERE t0 = 'J' and a1 = 9 ORDER BY t0, a1;

SET optimizer = on;

SELECT t0, a1, b2, filterValue FROM test_filter WHERE t0 = 'J' and a1 = 9 ORDER BY t0, a1;

-- start_ignore
{{ CLEAN_UP }}-- clean up resources
{{ CLEAN_UP }} DROP FOREIGN TABLE IF EXISTS test_filter CASCADE;
{{ CLEAN_UP }} DROP USER MAPPING IF EXISTS FOR CURRENT_USER SERVER pxf_filter_push_down_server;
{{ CLEAN_UP }} DROP SERVER IF EXISTS pxf_filter_push_down_server CASCADE;
{{ CLEAN_UP }} DROP FOREIGN DATA WRAPPER IF EXISTS pxf_filter_push_down_fdw;
-- end_ignore
