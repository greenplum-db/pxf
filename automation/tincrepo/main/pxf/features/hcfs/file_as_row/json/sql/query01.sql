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
select
       json_array_elements(json_blob->'root')->'record'->'created_at' as created_at,
       json_array_elements(json_blob->'root')->'record'->'text' as text,
       json_array_elements(json_blob->'root')->'record'->'user'->'name' as username,
       json_array_elements(json_blob->'root')->'record'->'user'->'screen_name' as screen_name,
       json_array_elements(json_blob->'root')->'record'->'user'->'location' as user_location
from file_as_row_json;
