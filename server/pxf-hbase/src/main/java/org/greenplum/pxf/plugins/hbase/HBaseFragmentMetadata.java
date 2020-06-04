package org.greenplum.pxf.plugins.hbase;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import org.apache.hadoop.hbase.HRegionInfo;
import org.codehaus.jackson.annotate.JsonCreator;
import org.greenplum.pxf.api.utilities.FragmentMetadata;

import java.util.Map;

public class HBaseFragmentMetadata implements FragmentMetadata {

    @Getter
    private final byte[] startKey;

    @Getter
    private final byte[] endKey;

    @Getter
    private final Map<String, byte[]> columnMapping;

    public HBaseFragmentMetadata(HRegionInfo region, Map<String, byte[]> columnMapping) {
        this(region.getStartKey(), region.getEndKey(), columnMapping);
    }

    @JsonCreator
    public HBaseFragmentMetadata(
            @JsonProperty("startKey") byte[] startKey,
            @JsonProperty("endKey") byte[] endKey,
            @JsonProperty("columnMapping") Map<String, byte[]> columnMapping) {
        this.startKey = startKey;
        this.endKey = endKey;
        this.columnMapping = columnMapping;
    }
}
