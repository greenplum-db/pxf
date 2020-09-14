package org.greenplum.pxf.plugins.hive;

import org.greenplum.pxf.api.model.BasePlugin;
import org.greenplum.pxf.plugins.hive.utilities.HiveUtilities;

/**
 * The base class for hive plugins
 */
public abstract class HivePlugin extends BasePlugin {

    protected HiveUtilities hiveUtilities;

    /**
     * Creates a {@code HivePlugin} with the provided {@code HiveUtilities}
     *
     * @param hiveUtilities the {@code HiveUtilities} instance
     */
    public HivePlugin(HiveUtilities hiveUtilities) {
        this.hiveUtilities = hiveUtilities;
    }
}
