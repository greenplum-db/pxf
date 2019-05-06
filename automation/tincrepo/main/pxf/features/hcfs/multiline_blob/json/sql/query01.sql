-- start_ignore
-- end_ignore
-- @description query01 tests that a multiline json file returns as a single multiline record in GPDB
--

-- Display on for output consistency between GPDB 5 and 6
\x on
select * from file_as_row_json;


-- Query JSON using JSON functions

select
       record->'record'->'created_at' as created_at,
       record->'record'->'text' as text,
       record->'record'->'user'->'name' as username,
       record->'record'->'user'->'screen_name' as screen_name,
       record->'record'->'user'->'location' as user_location
from file_as_row_json,
     json_array_elements(json_blob->'root') record;
