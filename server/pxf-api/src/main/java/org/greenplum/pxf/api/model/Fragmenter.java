package org.greenplum.pxf.api.model;

import java.util.List;

public interface Fragmenter extends Plugin {
    /**
     * Gets the fragments of a given path (source name and location of each
     * fragment). Used to get fragments of data that could be read in parallel
     * from the different segments.
     *
     * @return list of data fragments
     * @throws RuntimeException if fragment list could not be retrieved
     */
    List<Fragment> getFragments() throws Exception;

    /**
     * Default implementation of statistics for fragments. The default is:
     * <ul>
     * <li>number of fragments - as gathered by {@link #getFragments()}</li>
     * <li>first fragment size - 64MB</li>
     * <li>total size - number of fragments times first fragment size</li>
     * </ul>
     * Each fragmenter implementation can override this method to better match
     * its fragments stats.
     *
     * @return default statistics
     * @throws RuntimeException if statistics cannot be gathered
     */
    FragmentStats getFragmentStats() throws Exception;
}
