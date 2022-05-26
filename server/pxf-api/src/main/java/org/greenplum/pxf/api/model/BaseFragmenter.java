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

        String profile = context.getProfile();
        throw new UnsupportedOperationException(String.format("Profile '%s' does not support statistics for fragments", profile));
    }
}
