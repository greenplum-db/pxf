package org.greenplum.pxf.api.examples;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.greenplum.pxf.api.utilities.FragmentMetadata;

@NoArgsConstructor
public class DemoFragmentMetadata implements FragmentMetadata {

    @Getter
    @Setter
    private String path;

    public DemoFragmentMetadata(String path) {
        this.path = path;
    }
}
