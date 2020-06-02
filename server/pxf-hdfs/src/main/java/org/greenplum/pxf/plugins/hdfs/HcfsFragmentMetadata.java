package org.greenplum.pxf.plugins.hdfs;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import org.apache.hadoop.mapred.FileSplit;
import org.greenplum.pxf.api.utilities.FragmentMetadata;

public class HcfsFragmentMetadata implements FragmentMetadata {

    @Getter
    private final long start;

    @Getter
    private final long length;

    public HcfsFragmentMetadata(FileSplit fsp) {
        this(fsp.getStart(), fsp.getLength());
    }

    @JsonCreator
    public HcfsFragmentMetadata(
            @JsonProperty("start") long start,
            @JsonProperty("length") long length) {
        this.start = start;
        this.length = length;
    }
}
