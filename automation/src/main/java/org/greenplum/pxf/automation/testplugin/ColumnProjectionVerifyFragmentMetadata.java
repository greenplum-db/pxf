package org.greenplum.pxf.automation.testplugin;

import org.greenplum.pxf.api.utilities.FragmentMetadata;

public class ColumnProjectionVerifyFragmentMetadata implements FragmentMetadata {

    private String projection;

    public ColumnProjectionVerifyFragmentMetadata() {
    }

    public ColumnProjectionVerifyFragmentMetadata(String projection) {
        this.projection = projection;
    }

    public String getProjection() {
        return projection;
    }
}
