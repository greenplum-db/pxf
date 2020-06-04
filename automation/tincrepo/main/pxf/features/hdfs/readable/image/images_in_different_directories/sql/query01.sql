-- @description query01 for PXF HDFS Readable images

SELECT image_test_images_in_different_directories.names, image_test_images_in_different_directories.directories, image_test_images_in_different_directories.one_hot_encodings FROM image_test_images_in_different_directories, compare_table_images_in_different_directories
WHERE image_test_images_in_different_directories.fullpaths            = compare_table_images_in_different_directories.fullpaths
  AND image_test_images_in_different_directories.names                = compare_table_images_in_different_directories.names
  AND image_test_images_in_different_directories.directories          = compare_table_images_in_different_directories.directories
  AND image_test_images_in_different_directories.one_hot_encodings    = compare_table_images_in_different_directories.one_hot_encodings
  AND image_test_images_in_different_directories.dimensions           = compare_table_images_in_different_directories.dimensions
  AND image_test_images_in_different_directories.images               = compare_table_images_in_different_directories.images
ORDER BY names, directories;
SELECT image_test_images_in_different_directories_bytea.names, image_test_images_in_different_directories_bytea.directories, image_test_images_in_different_directories_bytea.one_hot_encodings FROM image_test_images_in_different_directories_bytea, compare_table_images_in_different_directories_bytea
WHERE image_test_images_in_different_directories_bytea.fullpaths         = compare_table_images_in_different_directories_bytea.fullpaths
  AND image_test_images_in_different_directories_bytea.names             = compare_table_images_in_different_directories_bytea.names
  AND image_test_images_in_different_directories_bytea.directories       = compare_table_images_in_different_directories_bytea.directories
  AND image_test_images_in_different_directories_bytea.one_hot_encodings = compare_table_images_in_different_directories_bytea.one_hot_encodings
  AND image_test_images_in_different_directories_bytea.dimensions        = compare_table_images_in_different_directories_bytea.dimensions
  AND image_test_images_in_different_directories_bytea.images            = compare_table_images_in_different_directories_bytea.images
ORDER BY names, directories;
