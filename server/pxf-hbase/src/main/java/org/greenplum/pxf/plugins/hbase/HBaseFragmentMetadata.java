package org.greenplum.pxf.plugins.hbase;

import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.greenplum.pxf.api.utilities.FragmentMetadata;

import java.util.Map;

/**
 * Fragment metadata for HBase profiles
 */
@Getter
@NoArgsConstructor
public class HBaseFragmentMetadata implements FragmentMetadata {

    private byte[] startKey;

    private byte[] endKey;

    private Map<String, byte[]> columnMapping;

    public HBaseFragmentMetadata(HRegionInfo region, Map<String, byte[]> columnMapping) {
        this(region.getStartKey(), region.getEndKey(), columnMapping);
    }

    public HBaseFragmentMetadata(byte[] startKey, byte[] endKey, Map<String, byte[]> columnMapping) {
        this.startKey = startKey;
        this.endKey = endKey;
        this.columnMapping = columnMapping;
    }
}
