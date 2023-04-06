package org.greenplum.pxf.service.serde;

import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.RequestContext;

import java.io.DataInput;
import java.util.Collections;
import java.util.List;

/**
 * Record reader that reads stores the whole input stream as the value of the first and only field of the resulting record.
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
