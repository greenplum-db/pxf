package org.greenplum.pxf.plugins.hdfs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.plugins.hdfs.utilities.HdfsUtilities;

import java.net.URI;
import java.util.List;

/**
 * Fragmenter class for file resources. This fragmenter
 * adds support for profiles that require files without
 * splits. The list of fragments will be the list of files
 * at the storage layer.
 */
public class HdfsFileFragmenter extends HdfsDataFragmenter {

    /**
     * Gets the fragments for a data source URI that can appear as a file name,
     * a directory name or a wildcard. Returns the data fragments in JSON
     * format.
     */
    @Override
    public List<Fragment> getFragments() throws Exception {
        String fileName = hcfsType.getDataUri(jobConf, context);
        Path path = new Path(fileName);
        // The hostname is no longer used, hardcoding it to localhost
        String[] hosts = {"localhost"};
        byte[] dummyMetadata = HdfsUtilities
                .prepareFragmentMetadata(0, 0, hosts);

        FileSystem fs = FileSystem.get(URI.create(fileName), configuration);
        RemoteIterator<LocatedFileStatus> fileStatusListIterator =
                fs.listFiles(path, false);

        while (fileStatusListIterator.hasNext()) {
            LocatedFileStatus fileStatus = fileStatusListIterator.next();
            String sourceName = fileStatus.getPath().toUri().toString();
            Fragment fragment = new Fragment(sourceName, hosts, dummyMetadata);
            fragments.add(fragment);
        }
        LOG.debug("Total number of fragments = {}", fragments.size());

        return fragments;
    }
}
