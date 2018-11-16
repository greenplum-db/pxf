package org.greenplum.pxf.plugins.hive;

import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.greenplum.pxf.api.model.HDFSPlugin;
import org.greenplum.pxf.plugins.hive.utilities.HiveUtilities;

public class HivePlugin extends HDFSPlugin {

    /**
     * @return ORC file reader
     */
    protected Reader getOrcReader() {
        return HiveUtilities.getOrcReader(configuration, inputData);
    }
}
