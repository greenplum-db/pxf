-- @description query01 for PXF HDFS Readable images

SELECT image_test_single_fragment.names, image_test_single_fragment.directories FROM image_test_single_fragment, compare_table_single_fragment
    WHERE image_test_single_fragment.fullpaths = compare_table_single_fragment.fullpaths
    AND image_test_single_fragment.directories = compare_table_single_fragment.directories
    AND image_test_single_fragment.names       = compare_table_single_fragment.names
    AND image_test_single_fragment.dimensions  = compare_table_single_fragment.dimensions
    AND image_test_single_fragment.images      = compare_table_single_fragment.images
    ORDER BY names;
SELECT image_test_single_fragment_bytea.names, image_test_single_fragment_bytea.directories FROM image_test_single_fragment_bytea, compare_table_single_fragment_bytea
    WHERE image_test_single_fragment_bytea.fullpaths = compare_table_single_fragment_bytea.fullpaths
    AND image_test_single_fragment_bytea.directories = compare_table_single_fragment_bytea.directories
    AND image_test_single_fragment_bytea.names       = compare_table_single_fragment_bytea.names
    AND image_test_single_fragment_bytea.dimensions  = compare_table_single_fragment_bytea.dimensions
    AND image_test_single_fragment_bytea.images      = compare_table_single_fragment_bytea.images
    ORDER BY names;
