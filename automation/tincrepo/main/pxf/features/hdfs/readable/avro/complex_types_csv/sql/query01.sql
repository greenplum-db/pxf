-- @description query01 for PXF HDFS Readable Avro supported complex type for FORMAT "CSV" test cases

SELECT type_long, type_string, type_array, type_union, type_enum, type_fixed from avrotest_complex_csv ORDER BY type_long;

CREATE view avro_view
as select type_string, string_to_array(substring(type_map from 2 for (char_length(type_map) - 2)),',')::text[]
as type_map, string_to_array(substring(type_record from 2 for (char_length(type_record) - 2)),',')::text[] as type_record from avrotest_complex_csv;

select exists(select type_map from avro_view where type_map <@ '{caleb:3,parham:3}' and type_map @> '{caleb:3,parham:3}');
select exists(select type_map from avro_view where type_map <@ '{kate:10,santosh:4}' and type_map @> '{kate:10,santosh:4}');
select exists(select type_map from avro_view where type_map <@ '{godon:1,jimmy:2}' and type_map @> '{godon:1,jimmy:2}');
select exists(select type_map from avro_view where type_map <@ '{karthik:3,prak:4,girish:3,arun:3}' and type_map @>'{karthik:3,prak:4,girish:3,arun:3}');

select exists(select type_record from avro_view where type_record <@ '{street:krishna street,number:12,city:chennai}' and type_record @> '{street:krishna street,number:12,city:chennai}');
select exists(select type_record from avro_view where type_record <@ '{street:melon ct,number:754,city:sunnyvale}'and type_record @> '{street:melon ct,number:754,city:sunnyvale}');
select exists(select type_record from avro_view where type_record <@ '{street:renaissance drive, number:1,city:san jose}' and type_record @> '{street:renaissance drive, number:1,city:san jose}');
select exists(select type_record from avro_view where type_record <@ '{street:deer creek,number:999,city:palo alto}' and type_record @> '{street:deer creek,number:999,city:palo alto}');

DROP VIEW avro_view CASCADE;
