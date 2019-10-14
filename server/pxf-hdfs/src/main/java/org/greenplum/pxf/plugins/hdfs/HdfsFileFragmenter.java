package org.greenplum.pxf.plugins.hdfs;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.plugins.hdfs.utilities.HdfsUtilities;

import java.io.IOException;
import java.net.URI;
import java.util.List;

/**
 * Fragmenter class for file resources. This fragmenter
 * adds support for profiles that require files without
 * splits. The list of fragments will be the list of files
 * at the storage layer.
 */
public class HdfsFileFragmenter extends HdfsDataFragmenter {

    // The hostname is no longer used, hardcoding it to localhost
    private static final String[] HOSTS = {"localhost"};
    private static byte[] DUMMY_METADATA;

    static {
        try {
            DUMMY_METADATA = HdfsUtilities.prepareFragmentMetadata(0, 0, HOSTS);
        } catch (IOException ignored) {
            // Should not fail
        }
    }

    /**
     * Gets the fragments for a data source URI that can appear as a file name,
     * a directory name or a wildcard. Returns the data fragments in JSON
     * format.
     */
    @Override
    public List<Fragment> getFragments() throws Exception {
        String fileName = hcfsType.getDataUri(jobConf, context);
        Path path = new Path(fileName);

        FileSystem fs = FileSystem.get(URI.create(fileName), configuration);
        FileStatus[] fileStatusList = fs.globStatus(path);

        for (FileStatus fileStatus : fileStatusList) {
            listFragments(fs, fileStatus.getPath());
        }
        LOG.debug("Total number of fragments = {}", fragments.size());

        return fragments;
    }

    /**
     * List fragments recursively, if a directory is found, recursively
     * list the files
     *
     * @param fs   the filesystem
     * @param path the path to list
     * @throws IOException for any IO error
     */
    private void listFragments(FileSystem fs, Path path) throws IOException {
        FileStatus[] fileStatusList = fs.listStatus(path);
        for (FileStatus fileStatus : fileStatusList) {
            if (fileStatus.isDirectory()) {
                listFragments(fs, fileStatus.getPath());
            } else {
                String sourceName = fileStatus.getPath().toUri().toString();
                Fragment fragment = new Fragment(sourceName, HOSTS, DUMMY_METADATA);
                fragments.add(fragment);
            }
        }
    }
}
