package org.greenplum.pxf.service.bridge;

import org.greenplum.pxf.api.model.RequestContext;

/**
 * Factory for creating instances of Bridge interfaces based on request context
 */
public interface BridgeFactory {

    /**
     * Returns an instance of the Bridge for reading data based on the request context
     * @param context request context
     * @return an instance of the Bridge suitable for a given request
     */
    Bridge getReadBridge(RequestContext context);

    /**
     * Returns an instance of the Bridge for writing data based on the request context
     * @param context request context
     * @return an instance of the Bridge suitable for a given request
     */
    Bridge getWriteBridge(RequestContext context);
}
