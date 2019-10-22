package org.greenplum.pxf.plugins.hdfs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.plugins.hdfs.utilities.HdfsUtilities;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Fragmenter class for file resources. This fragmenter
 * adds support for profiles that require files without
 * splits. The list of fragments will be the list of files
 * at the storage layer.
 */
public class HdfsFileFragmenter extends HdfsDataFragmenter {

    // The hostname is no longer used, hardcoding it to localhost
    private static final String[] HOSTS = {"localhost"};
    private static byte[] EMPTY_METADATA;

    static {
        try {
            EMPTY_METADATA = HdfsUtilities.prepareFragmentMetadata(0, 0, HOSTS);
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

        Path path = new Path(hcfsType.getDataUri(jobConf, context));
        fragments = getSplits(path)
                .stream()
                .filter(split -> ((FileSplit) split).getStart() == 0L)
                .map(split -> new Fragment(((FileSplit) split).getPath().toString(), HOSTS, EMPTY_METADATA))
                .collect(Collectors.toList());
        LOG.debug("Total number of fragments = {}", fragments.size());

        return fragments;
    }
}
