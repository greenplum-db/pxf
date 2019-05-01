-- start_ignore
-- end_ignore
-- @description query01 tests that a multiline json file returns as a single multiline record in GPDB
--

select * from multiline_blob_json;


-- Query JSON using JSON functions

select
       record->'record'->'created_at' as created_at,
       record->'record'->'text' as text,
       record->'record'->'user'->'name' as username,
       record->'record'->'user'->'screen_name' as screen_name,
       record->'record'->'user'->'location' as user_location
from multiline_blob_json,
     json_array_elements(json_blob->'root') record;
