package org.greenplum.pxf.service.bridge;

import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.ReadVectorizedResolver;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.Utilities;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

@Component
public class SimpleBridgeFactory implements BridgeFactory {

    private final ApplicationContext applicationContext;

    public SimpleBridgeFactory(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Bridge getBridge(RequestContext context, Configuration configuration) {

        Bridge bridge;
        if (context.getRequestType() == RequestContext.RequestType.READ_BRIDGE) {
            if (context.getStatsSampleRatio() > 0) {
                bridge = applicationContext.getBean(ReadSamplingBridge.class);
            } else if (Utilities.aggregateOptimizationsSupported(context)) {
                bridge = applicationContext.getBean(AggBridge.class);
            } else if (useVectorization(context)) {
                bridge = applicationContext.getBean(ReadVectorizedBridge.class);
            } else {
                bridge = applicationContext.getBean("readBridge", ReadBridge.class);
            }
        } else if (context.getRequestType() == RequestContext.RequestType.WRITE_BRIDGE) {
            bridge = applicationContext.getBean(WriteBridge.class);
        } else {
            throw new UnsupportedOperationException();
        }

        bridge.initialize(context, configuration);
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
