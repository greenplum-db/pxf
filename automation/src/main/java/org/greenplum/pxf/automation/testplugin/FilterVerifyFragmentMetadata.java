package org.greenplum.pxf.automation.testplugin;

import org.greenplum.pxf.api.utilities.FragmentMetadata;

public class FilterVerifyFragmentMetadata implements FragmentMetadata {

    private String filter;

    public FilterVerifyFragmentMetadata() {
    }

    public FilterVerifyFragmentMetadata(String filter) {
        this.filter = filter;
    }

    public String getFilter() {
        return filter;
    }
}
