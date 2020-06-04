package org.greenplum.pxf.api.model;

import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;

public interface StreamingFragmenter extends Fragmenter {
    /**
     * Mostly for testing, this method returns the list of directories
     * that were found after calling StreamingFragmenter#open()
     *
     * @return list of Paths representing directories
     */
    List<Path> getDirs();

    /**
     * After BasePlugin#initialize() is called, this method should be used
     * to get the list of directories that the StreamingFragmenter will use
     * to search for files.
     * <p>
     * Could throw different types of exceptions (not just IOException).
     *
     * @throws Exception since this will involve FileSystem operations
     */
    void open() throws Exception;

    /**
     * @return whether or not there are any more fragments
     * @throws IOException
     */
    boolean hasNext() throws IOException;

    /**
     * @return the next Fragment
     * @throws IOException
     */
    Fragment next() throws IOException;
}
