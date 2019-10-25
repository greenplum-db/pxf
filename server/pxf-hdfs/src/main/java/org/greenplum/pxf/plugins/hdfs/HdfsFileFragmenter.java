package org.greenplum.pxf.plugins.hdfs;

import org.apache.hadoop.fs.Path;
import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.plugins.hdfs.utilities.PxfInputFormat;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

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

        PxfInputFormat pxfInputFormat = new PxfInputFormat();
        PxfInputFormat.setInputPaths(jobConf, path);

        fragments = Arrays.stream(pxfInputFormat.listStatus(jobConf))
                .map(fileStatus -> new Fragment(fileStatus.getPath().toUri().toString()))
                .collect(Collectors.toList());
        LOG.debug("Total number of fragments = {}", fragments.size());

        return fragments;
    }
}
