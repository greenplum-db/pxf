package org.greenplum.pxf.automation.features;

import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.greenplum.pxf.automation.BaseFunctionality;

public abstract class BaseFeature extends BaseFunctionality {

    protected void createTable(ReadableExternalTable gpdbExternalTable) throws Exception {

        gpdbExternalTable.setHost(pxfHost);
        gpdbExternalTable.setPort(pxfPort);
        gpdb.createTableAndVerify(gpdbExternalTable);
    }

}