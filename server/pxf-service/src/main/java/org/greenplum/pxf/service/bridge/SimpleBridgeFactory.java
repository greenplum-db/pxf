package org.greenplum.pxf.service.bridge;

import org.greenplum.pxf.api.model.ReadVectorizedResolver;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.WriteVectorizedResolver;
import org.greenplum.pxf.api.utilities.Utilities;
import org.greenplum.pxf.service.serde.RecordReaderFactory;
import org.greenplum.pxf.service.utilities.BasePluginFactory;
import org.greenplum.pxf.service.utilities.GSSFailureHandler;
import org.springframework.stereotype.Component;

@Component
public class SimpleBridgeFactory implements BridgeFactory {

    private final BasePluginFactory pluginFactory;
    private final RecordReaderFactory recordReaderFactory;
    private final GSSFailureHandler failureHandler;

    public SimpleBridgeFactory(BasePluginFactory pluginFactory, RecordReaderFactory recordReaderFactory, GSSFailureHandler failureHandler) {
        this.pluginFactory = pluginFactory;
        this.recordReaderFactory = recordReaderFactory;
        this.failureHandler = failureHandler;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Bridge getBridge(RequestContext context) {

        Bridge bridge;
        if (context.getRequestType() == RequestContext.RequestType.WRITE_BRIDGE) {
            if (useWriteVectorization(context)) {
                bridge = new WriteVectorizedBridge(pluginFactory, recordReaderFactory, context, failureHandler);
            } else {
                bridge = new WriteBridge(pluginFactory, recordReaderFactory, context, failureHandler);
            }
        } else if (context.getRequestType() != RequestContext.RequestType.READ_BRIDGE) {
            throw new UnsupportedOperationException("Current Operation is not supported");
        } else if (context.getStatsSampleRatio() > 0) {
            bridge = new ReadSamplingBridge(pluginFactory, context, failureHandler);
        } else if (Utilities.aggregateOptimizationsSupported(context)) {
            bridge = new AggBridge(pluginFactory, context, failureHandler);
        } else if (useReadVectorization(context)) {
            bridge = new ReadVectorizedBridge(pluginFactory, context, failureHandler);
        } else {
            bridge = new ReadBridge(pluginFactory, context, failureHandler);
        }
        return bridge;
    }

    /**
     * Determines whether to use vectorization when reading data from an external system
     *
     * @param requestContext input protocol data
     * @return true if vectorization during reading is applicable in a current context
     */
    private boolean useReadVectorization(RequestContext requestContext) {
        String resolverName = requestContext.getResolver();
        return Utilities.implementsInterface(resolverName, ReadVectorizedResolver.class);
    }

    /**
     * Determines whether to use vectorization when writing data to an external system
     *
     * @param requestContext input protocol data
     * @return true if vectorization during writing is applicable in a current context
     */
    private boolean useWriteVectorization(RequestContext requestContext) {
        String resolverName = requestContext.getResolver();
        return Utilities.implementsInterface(resolverName, WriteVectorizedResolver.class);
    }

}
