package org.greenplum.pxf.service.serde;

import com.google.common.base.Preconditions;
import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.greenplum.pxf.api.model.OutputFormat;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.plugins.hdfs.utilities.PgUtilities;
import org.springframework.stereotype.Component;

/**
 * A factory that creates a new RecordReader to deserialize data from Greenplum. It looks into the information
 * in the RequestContext to decide which particular RecordReader to create.
 * This is a Spring Component that gets auto-wired into other Spring services.
 */
@Component
public class RecordReaderFactory {

    private final PgUtilities pgUtilities;

    /**
     * Creates a new instance of the factory.
     * @param pgUtilities utilities instance that helps with binary and array operations
     */
    public RecordReaderFactory(PgUtilities pgUtilities) {
        this.pgUtilities = pgUtilities;
    }

    /**
     * Creates a new RecordReader instance. The actual class implementing the RecordReader interface is decided
     * by inspecting the outputFormat ('TEXT' or 'GPDBWritable') that the provided RequestContext contains.
     * @param context the request context
     * @param canHandleInputStream true if the downstream resolver can handle an input stream, false otherwise
     * @return a new RecordReader implementation
     */
    public RecordReader getRecordReader(RequestContext context, boolean canHandleInputStream) {
        OutputFormat outputFormat = context.getOutputFormat();
        Preconditions.checkNotNull(outputFormat, "outputFormat is not set in RequestContext");
        switch (outputFormat) {
            case GPDBWritable:
                return new GPDBWritableRecordReader(context);
            case TEXT:
                if (canHandleInputStream) {
                    /*
                    If downstream components (resolver / accessor) can handle an inputStream directly, use a shortcut
                    to avoid reading bytes from the inputStream here and instead pass the inputStream in the record.
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
