package org.greenplum.pxf.service.bridge;

import org.greenplum.pxf.api.io.Writable;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.StreamingResolver;
import org.greenplum.pxf.api.utilities.AccessorFactory;
import org.greenplum.pxf.api.utilities.ResolverFactory;
import org.greenplum.pxf.service.BridgeOutputBuilder;

import java.io.DataInputStream;

/**
 * A read bridge for co-ordinating the StreamingImageAccessor and StreamingImageResolver,
 * it obtains an iterator from the BridgeOutputBuilder which is used to send data to BridgeResource
 * for writing.
 * <p>
 * BridgeResource#readResponse() calls getNext() until null is returned, which terminates the record.
 */
public class StreamingReadBridge extends BaseBridge {
    final BridgeOutputBuilder outputBuilder;
    BridgeOutputBuilder.WritableIterator writableIterator;

    public StreamingReadBridge(RequestContext context) {
        this(context, AccessorFactory.getInstance(), ResolverFactory.getInstance());
    }

    StreamingReadBridge(RequestContext context, AccessorFactory accessorFactory, ResolverFactory resolverFactory) {
        super(context, accessorFactory, resolverFactory);
        outputBuilder = new BridgeOutputBuilder(context);
    }

    @Override
    public boolean beginIteration() throws Exception {
        if (!StreamingResolver.class.isAssignableFrom(resolver.getClass())) {
            throw new Exception("StreamingReadBridge requires a streaming resolver type");
        }

        boolean result = accessor.openForRead();

        writableIterator = outputBuilder.new WritableIterator(resolver.getFields(accessor.readNextObject()));

        return result;
    }

    @Override
    public Writable getNext() throws Exception {
        return writableIterator.hasNext() ? writableIterator.next() : null;
    }

    @Override
    public boolean isThreadSafe() {
        return true;
    }

    @Override
    public void endIteration() throws Exception {
        try {
            accessor.closeForRead();
        } catch (Exception e) {
            LOG.error("Failed to close bridge resources: {}", e.getMessage());
            throw e;
        }
    }

    @Override
    public boolean setNext(DataInputStream inputStream) {
        throw new UnsupportedOperationException("setNext is not implemented");
    }
}
