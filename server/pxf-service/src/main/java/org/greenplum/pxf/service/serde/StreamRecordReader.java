package org.greenplum.pxf.service.serde;

import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.RequestContext;

import java.io.DataInput;
import java.util.Collections;
import java.util.List;

/**
 * A RecordReader that does not actually read data, but instead stores the whole input stream as the value of
 * the first and only field of the resulting record. The data will be read by downstream components from the input
 * stream directly.
 *
 * This is a performance optimization used, for example, by the StringPassResolver and LineBreakAccessor to not break
 * the incoming stream into records and instead just copy incoming bytes to the external system.
 */
public class StreamRecordReader extends BaseRecordReader implements RecordReader {

    /**
     * Creates a new instance
     * @param context request context
     */
    public StreamRecordReader(RequestContext context) {
        super(context);
    }

    @Override
    public List<OneField> readRecord(DataInput input) {
        return Collections.singletonList(new OneField(DataType.BYTEA.getOID(), input));
    }
}
