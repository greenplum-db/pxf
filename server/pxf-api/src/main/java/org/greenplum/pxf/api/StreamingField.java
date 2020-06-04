package org.greenplum.pxf.api;

import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.StreamingResolver;

/**
 * A OneField containing a reference to a StreamingResolver,
 * and can thus be used to fetch data in a lazy manner using
 * StreamingResolver#next().
 */
public class StreamingField extends OneField {
    protected StreamingResolver resolver;

    public StreamingField(int type, StreamingResolver streamingResolver) {
        super(type, null);
        resolver = streamingResolver;
        if (type == DataType.BYTEA.getOID()) {
            this.setPrefix("\\x");
        }
    }

    public StreamingResolver getResolver() {
        return resolver;
    }
}
