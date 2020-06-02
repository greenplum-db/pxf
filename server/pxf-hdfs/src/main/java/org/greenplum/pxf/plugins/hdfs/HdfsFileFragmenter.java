package org.greenplum.pxf.plugins.hdfs;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InvalidInputException;
import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.plugins.hdfs.utilities.PxfInputFormat;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Fragmenter class for file resources. This fragmenter
 * adds support for profiles that require files without
 * splits. The list of fragments will be the list of files
 * at the storage layer.
 */
@Component("HdfsFileFragmenter")
@RequestScope
public class HdfsFileFragmenter extends HdfsDataFragmenter {

    /**
     * Gets the fragments for a data source URI that can appear as a file name,
     * a directory name or a wildcard. Returns the data fragments in JSON
     * format.
     */
    @Override
    public List<Fragment> getFragments() throws Exception {
        // TODO: make sure jobConf is not needed or revert to hcfsType.getDataUri(jobConf, context);
        String fileName = hcfsType.getDataUri(context);
        Path path = new Path(fileName);

        PxfInputFormat pxfInputFormat = new PxfInputFormat();
        PxfInputFormat.setInputPaths(jobConf, path);

        FileStatus[] fileStatusArray;

        try {
            fileStatusArray = pxfInputFormat.listStatus(jobConf);
        } catch (InvalidInputException e) {
            if (StringUtils.equalsIgnoreCase("true", context.getOption(IGNORE_MISSING_PATH_OPTION))) {
                LOG.debug("Ignoring InvalidInputException", e);
                return fragments;
            }
            throw e;
        }

        fragments = Arrays.stream(fileStatusArray)
                .map(fileStatus -> new Fragment(fileStatus.getPath().toUri().toString()))
                .collect(Collectors.toList());
        LOG.debug("Total number of fragments = {}", fragments.size());

        return fragments;
    }
}
