package org.greenplum.pxf.api;

import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.StreamingResolver;

/**
 * A special case of a Streaming field that represents an array.
 * Each subsequent call of StreamingResolver#next() should be expected to return
 * an element of the array.
 */
public class ArrayStreamingField extends StreamingField {
    private String separator = ",";

    public ArrayStreamingField(StreamingResolver streamingResolver) {
        super(DataType.TEXTARRAY.getOID(), null);
        this.setPrefix("{");
        this.setSuffix("}");
        resolver = streamingResolver;
    }

    public String getSeparator() {
        return this.separator;
    }

    public void setSeparator(String separator) {
        this.separator = separator;
    }
}
