package org.greenplum.pxf.service.serde;

import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.plugins.hdfs.utilities.PgUtilities;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class RecordReaderFactory {

    private final PgUtilities pgUtilities;

    public RecordReaderFactory(PgUtilities pgUtilities) {
        this.pgUtilities = pgUtilities;
    }

    public RecordReader getRecordReader(RequestContext context, boolean canHandleInputStream) {
        switch (context.getOutputFormat()) {
            case GPDBWritable:
                return new GPDBWritableRecordReader(context);
            case TEXT:
                if (canHandleInputStream) {
                    /*
                    If downstream components (resolvers) can handle the input stream directly, use a shortcut and
                    avoid copying the bytes from the inputStream here and instead pass the inputStream in the record.
                    This code used to use the Text class to read bytes until a line delimiter was found. This would cause
                    issues with wide rows that had 1MB+, because the Text code grows the array to fit data, and
                    it does it inefficiently. We observed multiple calls to System.arraycopy in the setCapacity method
                    for every byte after we exceeded the original buffer size. This caused terrible performance in PXF,
                    even when writing a single row to an external system.
                    */
                    return new StreamRecordReader(context);
                } else {
                    return new TextRecordReader(context, pgUtilities);
                }
            default:
                // in case there are more formats in the future and this class is not updated
                throw new PxfRuntimeException("Unsupported output format " + context.getOutputFormat());
        }
    }
}
