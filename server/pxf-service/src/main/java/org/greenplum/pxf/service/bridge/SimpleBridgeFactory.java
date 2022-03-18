package org.greenplum.pxf.service.bridge;

import org.greenplum.pxf.api.model.ReadVectorizedResolver;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.Utilities;
import org.greenplum.pxf.service.utilities.BasePluginFactory;
import org.greenplum.pxf.service.utilities.GSSFailureHandler;
import org.springframework.stereotype.Component;

@Component
public class SimpleBridgeFactory implements BridgeFactory {

    private final BasePluginFactory pluginFactory;
    private final GSSFailureHandler failureHandler;

    public SimpleBridgeFactory(BasePluginFactory pluginFactory, GSSFailureHandler failureHandler) {
        this.pluginFactory = pluginFactory;
        this.failureHandler = failureHandler;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Bridge getBridge(RequestContext context) {

        Bridge bridge;
        if (context.getRequestType() == RequestContext.RequestType.WRITE_BRIDGE) {
            bridge = new WriteBridge(pluginFactory, context, failureHandler);
        } else if (context.getRequestType() != RequestContext.RequestType.READ_BRIDGE) {
            throw new UnsupportedOperationException("Current Operation is not supported");
        } else if (context.getStatsSampleRatio() > 0) {
            bridge = new ReadSamplingBridge(pluginFactory, context, failureHandler);
        } else if (Utilities.aggregateOptimizationsSupported(context)) {
            bridge = new AggBridge(pluginFactory, context, failureHandler);
        } else if (useVectorization(context)) {
            bridge = new ReadVectorizedBridge(pluginFactory, context, failureHandler);
        } else {
            bridge = new ReadBridge(pluginFactory, context, failureHandler);
        }
        return bridge;
    }

    /**
     * Determines whether to use vectorization
     *
     * @param requestContext input protocol data
     * @return true if vectorization is applicable in a current context
     */
    private boolean useVectorization(RequestContext requestContext) {
        return Utilities.implementsInterface(requestContext.getResolver(), ReadVectorizedResolver.class);
    }

}
