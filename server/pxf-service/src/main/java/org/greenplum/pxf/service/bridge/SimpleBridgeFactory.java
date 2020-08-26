package org.greenplum.pxf.service.bridge;

import org.greenplum.pxf.api.ReadVectorizedResolver;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.Utilities;
import org.greenplum.pxf.service.utilities.BasePluginFactory;
import org.springframework.stereotype.Component;

@Component
public class SimpleBridgeFactory implements BridgeFactory {

    private final BasePluginFactory pluginFactory;

    public SimpleBridgeFactory(BasePluginFactory pluginFactory) {
        this.pluginFactory = pluginFactory;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Bridge getBridge(RequestContext context) {

        Bridge bridge;
        if (context.getRequestType() == RequestContext.RequestType.WRITE_BRIDGE) {
            bridge = new WriteBridge(pluginFactory, context);
        } else if (context.getRequestType() != RequestContext.RequestType.READ_BRIDGE) {
            throw new UnsupportedOperationException();
        } else if (context.getStatsSampleRatio() > 0) {
            bridge = new ReadSamplingBridge(pluginFactory, context);
        } else if (Utilities.aggregateOptimizationsSupported(context)) {
            bridge = new AggBridge(pluginFactory, context);
        } else if (useVectorization(context)) {
            bridge = new ReadVectorizedBridge(pluginFactory, context);
        } else {
            bridge = new ReadBridge(pluginFactory, context);
        }
        return bridge;
    }

    /**
     * Determines whether use vectorization
     *
     * @param requestContext input protocol data
     * @return true if vectorization is applicable in a current context
     */
    private boolean useVectorization(RequestContext requestContext) {
        return Utilities.implementsInterface(requestContext.getResolver(), ReadVectorizedResolver.class);
    }

}
