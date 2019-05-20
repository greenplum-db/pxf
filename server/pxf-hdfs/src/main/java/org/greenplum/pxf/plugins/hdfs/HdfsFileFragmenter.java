package org.greenplum.pxf.plugins.hdfs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.greenplum.pxf.api.model.BaseFragmenter;
import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.FragmentMetadata;
import org.greenplum.pxf.plugins.hdfs.utilities.HdfsUtilities;

import java.net.URI;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Fragmenter class for file resources. This fragmenter
 * adds support for profiles that require files without
 * splits. The list of fragments will be the list of files
 * at the storage layer.
 */
public class HdfsFileFragmenter extends BaseFragmenter {

    private HcfsType hcfsType;

    /*
     * Keeps track of the number of getFragments calls made
     * during the lifetime of the application
     */
    static AtomicLong fragmenterAccessCount = new AtomicLong(0L);

    @Override
    public void initialize(RequestContext context) {
        super.initialize(context);

        // Check if the underlying configuration is for HDFS
        hcfsType = HcfsType.getHcfsType(configuration, context);
    }

    /**
     * Gets the fragments for a data source URI that can appear as a file name,
     * a directory name or a wildcard. Returns the data fragments in JSON
     * format.
     */
    @Override
    public List<Fragment> getFragments() throws Exception {
        long fragmenterAccessCountCurrent = fragmenterAccessCount.incrementAndGet();

        if (fragmenterAccessCountCurrent % 100 == 0) {
            LOG.debug("HdfsFileFragmenter has been invoked {} times during the lifetime of this application",
                    fragmenterAccessCountCurrent);
        }

        String fileName = hcfsType.getDataUri(configuration, context);
        Path path = new Path(fileName);
        // The hostname is not used anymore, so we hardcode it to localhost
        String[] hosts = {"localhost"};
        byte[] dummyMetadata = HdfsUtilities
                .prepareFragmentMetadata(0, Integer.MAX_VALUE, hosts);

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
