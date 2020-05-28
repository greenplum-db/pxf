package org.greenplum.pxf.api.model;

import org.springframework.beans.factory.annotation.Autowired;

import java.util.LinkedList;
import java.util.List;

public class BaseFragmenter implements Fragmenter {

    protected List<Fragment> fragments = new LinkedList<>();

    protected RequestContext context;

    @Autowired
    public void setRequestContext(RequestContext context) {
        this.context = context;
    }

    @Override
    public List<Fragment> getFragments() throws Exception {
        return fragments;
    }

    @Override
    public FragmentStats getFragmentStats() throws Exception {
        throw new UnsupportedOperationException("Operation getFragmentStats is not supported");
    }
}
