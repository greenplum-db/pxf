-- @description query01 for PXF HDFS Readable images
-- @description query01 for PXF HDFS Readable images
-- start_matchsubs
--
-- # create a match/subs
--
-- m/DETAIL/
-- s/DETAIL/CONTEXT/
-- m/from '.*:.*': +type/
-- s/from '.*:.*': +type/from 'HOST:PORT': type/
-- m/Unable to read image data in .*\/invalid_image_directory\/invalid_image.png.*/
-- s/Unable to read image data in .*\/invalid_image_directory\/invalid_image.png.*/Unable to read image data in invalid_image_directory\/invalid_image.png/
-- m/line .* of file pxf:.*\?PROFILE=hdfs:image&FILES_PER_FRAGMENT=1/
-- s/line .* of file pxf:.*\?PROFILE=hdfs:image&FILES_PER_FRAGMENT=1/line X of file pxf:\/\/path_to_file?PROFILE=hdfs:image&FILES_PER_FRAGMENT=1/
-- end_matchsubs
SELECT badly_sized_image_on_path.names FROM badly_sized_image_on_path;
SELECT badly_sized_image_on_path_bytea.names FROM badly_sized_image_on_path_bytea;
