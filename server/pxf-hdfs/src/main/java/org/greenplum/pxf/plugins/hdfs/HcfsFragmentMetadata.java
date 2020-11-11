package org.greenplum.pxf.plugins.hdfs;

import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.hadoop.mapred.FileSplit;
import org.greenplum.pxf.api.utilities.FragmentMetadata;

@NoArgsConstructor
public class HcfsFragmentMetadata implements FragmentMetadata {

    @Getter
    protected long start;

    @Getter
    protected long length;

    public HcfsFragmentMetadata(FileSplit fsp) {
        this(fsp.getStart(), fsp.getLength());
    }

    public HcfsFragmentMetadata(long start, long length) {
        this.start = start;
        this.length = length;
    }
}
