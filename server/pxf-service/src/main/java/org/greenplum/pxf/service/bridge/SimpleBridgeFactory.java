package org.greenplum.pxf.service.bridge;

import org.greenplum.pxf.api.ReadVectorizedResolver;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.Utilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleBridgeFactory implements BridgeFactory {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleBridgeFactory.class);
    private static final SimpleBridgeFactory instance = new SimpleBridgeFactory();

    /**
     * Returns a singleton instance of the factory.
     *
     * @return a singleton instance of the factory.
     */
    public static BridgeFactory getInstance() {
        return instance;
    }

    @Override
    public Bridge getReadBridge(RequestContext context) {

        Bridge bridge;
        if (context.getStatsSampleRatio() > 0) {
            bridge = new ReadSamplingBridge(context);
        } else if (Utilities.aggregateOptimizationsSupported(context)) {
            bridge = new AggBridge(context);
        } else if (useVectorization(context)) {
            bridge = new ReadVectorizedBridge(context);
        } else {
            bridge = new ReadBridge(context);
        }
        return bridge;
    }

    @Override
    public Bridge getWriteBridge(RequestContext context) {
        return new WriteBridge(context);
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
