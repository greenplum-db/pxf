package org.greenplum.pxf.automation.testplugin;

import org.greenplum.pxf.api.utilities.FragmentMetadata;

public class DummyFragmentMetadata implements FragmentMetadata {

    private String s;

    public DummyFragmentMetadata() {
    }

    public DummyFragmentMetadata(String s) {
        this.s = s;
    }

    public String getS() {
        return s;
    }
}
