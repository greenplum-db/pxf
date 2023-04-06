package org.greenplum.pxf.service.bridge;

import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.error.BadRecordException;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.WriteVectorizedResolver;
import org.greenplum.pxf.service.serde.RecordReaderFactory;
import org.greenplum.pxf.service.utilities.BasePluginFactory;
import org.greenplum.pxf.service.utilities.GSSFailureHandler;

import java.io.DataInputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * A WriteBridge that reads multiple records from the InputStream into a batch and then uses a vectorized resolver and
 * accessor to write it to the remote system.
 */
public class WriteVectorizedBridge extends WriteBridge {

    /**
     * Creates a new instance of the bridge.
     * @param pluginFactory plugin factory
     * @param recordReaderFactory factory for creating a record reader to deserialize incoming data
     * @param context request context
     * @param failureHandler failure handler
     */
    public WriteVectorizedBridge(BasePluginFactory pluginFactory, RecordReaderFactory recordReaderFactory,
                                 RequestContext context, GSSFailureHandler failureHandler) {
        super(pluginFactory, recordReaderFactory, context, failureHandler);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean setNext(DataInputStream inputStream) throws Exception {
        // will be using VectorizedResolver which needs a batch or record to resolve at once
        // so need to accumulate a set of records before passing them to the resolver
        WriteVectorizedResolver vectorizedResolver = getVectorizedResolver();
        int batchSize = vectorizedResolver.getBatchSize();

        // TODO: should we re-use / reset a list ? what if the batch size ends up being different between iterations ?
        List<List<OneField>> batch = new ArrayList<>(batchSize);
        int recordCount = 0;
        while (recordCount < batchSize) {
            List<OneField> record = recordReader.readRecord(inputStream);
            if (record == null) {
                break; // no more records to read
            }
            batch.add(record); // add record to the batch
            recordCount++;
        }

        // resolve the whole batch
        OneRow resolvedBatch = vectorizedResolver.setFieldsForBatch(batch);
        if (resolvedBatch == null) {
            return false; // this will terminate further reading, might happen if the batch is empty
        }

        // write the resolved batch into the remote system
        if (!accessor.writeNextObject(resolvedBatch)) {
            throw new BadRecordException();
        }

        // if we read as many records as the batch size, there might be more data,
        // return true to indicate reading should continue, otherwise false to indicate iterations should stop
        return recordCount == batchSize;
    }

    /**
     * A resolver can potentially be changed between iterations by the failureHandler, check the type and cast it
     * @return an instance of the WriteVectorizedResolver to use for processing
     */
    private WriteVectorizedResolver getVectorizedResolver() {
        if (resolver instanceof WriteVectorizedResolver) {
            return (WriteVectorizedResolver) resolver;
        }
        throw new IllegalStateException(
                String.format("Resolver %s must implement WriteVectorizedResolver interface",
                        resolver.getClass().getName()));
    }
}
