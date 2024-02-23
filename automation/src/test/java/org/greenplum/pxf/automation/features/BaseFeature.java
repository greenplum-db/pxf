package org.greenplum.pxf.automation.features;

import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.greenplum.pxf.automation.BaseFunctionality;
import org.greenplum.pxf.automation.utils.jsystem.report.ReportUtils;
import org.postgresql.util.PSQLException;

public abstract class BaseFeature extends BaseFunctionality {

    protected void createTable(ReadableExternalTable gpdbExternalTable) throws Exception {

        gpdbExternalTable.setHost(pxfHost);
        gpdbExternalTable.setPort(pxfPort);
        gpdb.createTableAndVerify(gpdbExternalTable);
    }

    public interface ThrowingConsumer<E extends Exception> {
        void accept() throws E;
    }

    public void attemptInsert(ThrowingConsumer operation, String path, Integer retryCount) throws Exception {
        boolean success = false;
        for (int i = retryCount; i > 0; i--) {
            if (success) {
                break;
            }
            try {
                operation.accept();
                success = true;
            } catch (PSQLException e) {
                if (retryCount == 0) {
                    // if we're on the last retry, throw the error we got
                    throw e;
                } else if (e.getMessage().contains("Operation could not be completed within the specified time")) {
                    ReportUtils.startLevel(null, getClass(), String.format("Operation timed out, trying again. Retries left: %d : '%s'", i, e.getMessage()));


                    // operation timed out in the middle and not all the data was added, delete all the files and try again
                    hdfs.removeDirectory(path);
                }
            }
        }
    }
}
