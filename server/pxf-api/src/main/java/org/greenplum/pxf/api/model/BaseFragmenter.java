package org.greenplum.pxf.api.model;

import java.util.LinkedList;
import java.util.List;

public class BaseFragmenter extends BasePlugin implements Fragmenter {

    protected List<Fragment> fragments = new LinkedList<>();

    @Override
    public List<Fragment> getFragments() throws Exception {
        return fragments;
    }

    @Override
    public FragmentStats getFragmentStats() throws Exception {
        throw new UnsupportedOperationException("Operation getFragmentStats is not supported");
//        long fragmentsNumber = fragments.size();
//        return new FragmentStats(fragmentsNumber,
//                FragmentStats.DEFAULT_FRAGMENT_SIZE, fragmentsNumber
//                        * FragmentStats.DEFAULT_FRAGMENT_SIZE);
    }
}
