package org.greenplum.pxf.diagnostic;

import org.greenplum.pxf.api.model.BaseFragmenter;
import org.greenplum.pxf.api.model.Fragment;

import java.util.List;

/**
 * Test class for regression tests generates a list of fragments with the size equal to the number of segments
 * (as provided by the request header) such that each segment will have 1 fragment of data to read.
 */
public class EachSegmentDataFragmenter extends BaseFragmenter {

    /**
     * Returns a list where there is one fragment for each segment.
     *
     * @return a list where there is one fragment for each segment
     */
    @Override
    public List<Fragment> getFragments() throws Exception {
        for (int i = 0; i < context.getTotalSegments(); i++) {
            fragments.add(new Fragment(String.format("dummy_file_path_%s", i), null));
        }
        return fragments;
    }
}
