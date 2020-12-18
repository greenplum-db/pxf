package org.greenplum.pxf.plugins.hive;

import org.greenplum.pxf.api.model.ProtocolHandler;
import org.greenplum.pxf.api.model.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of ProtocolHandler for "hive" protocol.
 */
public class HiveProtocolHandler implements ProtocolHandler {

    private static final Logger LOG = LoggerFactory.getLogger(HiveProtocolHandler.class);
    private static final String HIVE_VECTORIZED_ORC_ACCESSOR = HiveORCVectorizedAccessor.class.getName();
    private static final String HIVE_VECTORIZED_ORC_RESOLVER = HiveORCVectorizedResolver.class.getName();
    private static final String OPTION_VECTORIZE = "VECTORIZE";

    @Override
    public String getFragmenterClassName(RequestContext context) {
        return context.getFragmenter(); // default to fragmenter defined by the profile
    }

    @Override
    public String getAccessorClassName(RequestContext context) {
        // default to accessor defined by the profile, switch to vectorized if requested by the user
        String accessor = useHiveVectorizedORC(context) ? HIVE_VECTORIZED_ORC_ACCESSOR : context.getAccessor();
        LOG.debug("Determined to use {} accessor", accessor);
        return accessor;
    }

    @Override
    public String getResolverClassName(RequestContext context) {
        // default to resolver defined by the profile, switch to vectorized if requested by the user
        String resolver = useHiveVectorizedORC(context) ? HIVE_VECTORIZED_ORC_RESOLVER : context.getResolver();
        LOG.debug("Determined to use {} resolver", resolver);
        return resolver;
    }

    /**
     * Determines whether the user has requested to use vectorized ORC accessor / resolver
     * @param context request context
     * @return true if vectorized ORC accessor and resolver will need to be used
     */
    private boolean useHiveVectorizedORC(RequestContext context) {
        return ("hive:orc".equals(context.getProfile()) && context.getOption(OPTION_VECTORIZE, false));
    }

}
