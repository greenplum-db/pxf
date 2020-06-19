package org.greenplum.pxf.plugins.hive;

import org.greenplum.pxf.api.model.BasePlugin;
import org.greenplum.pxf.plugins.hive.utilities.HiveUtilities;
import org.springframework.beans.factory.annotation.Autowired;

public class HivePlugin extends BasePlugin {

    protected HiveUtilities hiveUtilities;

    /**
     * Sets the {@link HiveUtilities} object
     *
     * @param hiveUtilities the hive utilities object
     */
    @Autowired
    public void setHiveUtilities(HiveUtilities hiveUtilities) {
        this.hiveUtilities = hiveUtilities;
    }
}
