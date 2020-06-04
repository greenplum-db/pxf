package org.greenplum.pxf.plugins.hdfs;

import org.apache.hadoop.fs.Path;
import org.greenplum.pxf.api.UnsupportedTypeException;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.plugins.hdfs.utilities.PxfInputFormat;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class HdfsMultiFileFragmenter extends HdfsDataFragmenter {
    protected int filesPerFragment;
    public final static String FILES_PER_FRAGMENT_OPTION_NAME = "FILES_PER_FRAGMENT";

    public int getFilesPerFragment() {
        return filesPerFragment;
    }

    @Override
    public void initialize(RequestContext context) {
        super.initialize(context);
        filesPerFragment = 1;
        final String filesPerFragmentOptionString = context.getOption(FILES_PER_FRAGMENT_OPTION_NAME);
        if (filesPerFragmentOptionString != null) {
            filesPerFragment = Integer.parseInt(filesPerFragmentOptionString);
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

        PxfInputFormat pxfInputFormat = new PxfInputFormat();
        PxfInputFormat.setInputPaths(jobConf, path);

        final List<String> files = Arrays
                .stream(pxfInputFormat.listStatus(jobConf))
                .map(fs -> fs.getPath().toUri().toString())
                .sorted()
                .collect(Collectors.toList());

        StringBuilder pathList = new StringBuilder();
        for (int i = 1; i <= files.size(); i++) {
            pathList.append(files.set(i - 1, null)).append(",");
            if (i % filesPerFragment == 0 || i == files.size()) {
                pathList.setLength(pathList.length() - 1);
                fragments.add(new Fragment(pathList.toString()));
                pathList.setLength(0);
                LOG.debug("Completed fragment batch #{}", (i - 1) / filesPerFragment);
            }
        }

        LOG.debug("Total number of fragments = {}", fragments.size());

        return fragments;
    }
}
