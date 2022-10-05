package org.greenplum.pxf.plugins.json;

import org.greenplum.pxf.api.model.ProtocolHandler;
import org.greenplum.pxf.api.model.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.commons.lang.StringUtils.isNotEmpty;

/**
 * Implementation of ProtocolHandler for json protocol.
 */
public class JsonProtocolHandler implements ProtocolHandler {

    private static final Logger LOG = LoggerFactory.getLogger(JsonProtocolHandler.class);
    private static final String HCFS_FILE_FRAGMENTER = "org.greenplum.pxf.plugins.hdfs.HdfsFileFragmenter";

    @Override
    public String getFragmenterClassName(RequestContext context) {
        String fragmenter = context.getFragmenter(); // default to fragmenter defined by the profile
        if (useMultilineJson(context)) {
            fragmenter = HCFS_FILE_FRAGMENTER;
        }
        LOG.debug("Determined to use {} fragmenter", fragmenter);
        return fragmenter;
    }

    @Override
    public String getAccessorClassName(RequestContext context) {
        return context.getAccessor();
    }

    @Override
    public String getResolverClassName(RequestContext context) {
        return context.getResolver();
    }

    public boolean useMultilineJson(RequestContext context) {
        return isNotEmpty(context.getOption("identifier"));
    }
}
