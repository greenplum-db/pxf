package org.greenplum.pxf.api.examples;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.greenplum.pxf.api.utilities.FragmentMetadata;

@AllArgsConstructor
public class DemoFragmentMetadata implements FragmentMetadata {

    @Getter
    @Setter
    private String path;
}
