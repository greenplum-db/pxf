package org.greenplum.pxf.automation.features;

import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.greenplum.pxf.automation.structures.tables.pxf.WritableExternalTable;

public class BaseWritableFeature extends BaseFeature {

    protected WritableExternalTable writableExTable;
    protected ReadableExternalTable readableExTable;
    // path in hdfs for writable output
    protected String hdfsWritePath;
    protected String writableTableName = "writable_table";
    protected String readableTableName = "readable_table";

    /**
     * Set writable directory
     */
    @Override
    protected void beforeClass() throws Exception {
        super.beforeClass();
        hdfsWritePath = hdfs.getWorkingDirectory() + "/writable_results/";
    }

    /**
     *  clean writable directory
     */
    @Override
    protected void afterMethod() throws Exception {
        super.afterMethod();
        hdfs.removeDirectory(hdfsWritePath);
    }
}
