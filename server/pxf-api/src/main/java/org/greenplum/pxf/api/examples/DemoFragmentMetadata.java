package org.greenplum.pxf.api.examples;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;
import org.greenplum.pxf.api.utilities.FragmentMetadata;

public class DemoFragmentMetadata implements FragmentMetadata {

    @Getter
    @Setter
    private String path;

    @JsonCreator
    public DemoFragmentMetadata(@JsonProperty("path") String path) {
        this.path = path;
    }
}
