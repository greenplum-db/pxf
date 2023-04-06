package org.greenplum.pxf.service.serde;

import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.List;

/**
 * Base class for record readers, stores request context and a few commonly used properties from it.
 */
public abstract class BaseRecordReader {

    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    protected final RequestContext context;
    protected final List<ColumnDescriptor> columnDescriptors;
    protected final Charset databaseEncoding;

    /**
     * Creates a new instance
     * @param context request context
     */
    public BaseRecordReader(RequestContext context) {
        this.context = context;
        columnDescriptors = context.getTupleDescription();
        databaseEncoding = context.getDatabaseEncoding();
    }
}
