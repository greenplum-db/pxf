-- start_ignore
-- end_ignore
-- @description query01 tests that a multiline json file returns as a single multiline record in GPDB
--

-- Display on for output consistency between GPDB 5 and 6
\x on
\pset format unaligned
select * from file_as_row_json;


-- Query JSON using JSON functions
\pset format aligned
select data -> 'record' -> 'created_at' as created_at ,
       data -> 'record' -> 'text' as text,
       data -> 'record' -> 'user'->'name' as username,
       data -> 'record' -> 'user'->'screen_name' as screen_name,
       data -> 'record' -> 'user'->'location' as user_location
from file_as_row_json fr, json_array_elements(fr.json_blob -> 'root') data;
