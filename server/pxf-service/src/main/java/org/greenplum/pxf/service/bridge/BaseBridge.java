package org.greenplum.pxf.service.bridge;

import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.Resolver;
import org.greenplum.pxf.api.utilities.AccessorFactory;
import org.greenplum.pxf.api.utilities.ResolverFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract class representing the bridge that provides to subclasses logger and accessor and
 * resolver instances obtained from the factories.
 */
public abstract class BaseBridge implements Bridge {

    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    protected Accessor accessor;
    protected Resolver resolver;

    /**
     * Creates a new instance for a given request context. Uses default singleton instances of
     * plugin factories to request accessor and resolver.
     * @param context request context
     */
    public BaseBridge(RequestContext context) {
        this(context, AccessorFactory.getInstance(), ResolverFactory.getInstance());
    }

    /**
     * Creates a new instance for a given request context. Uses provides instances of
     * plugin factories to request accessor and resolver.
     * @param context request context
     * @param accessorFactory accessor factory
     * @param resolverFactory resolver factory
     */
    BaseBridge(RequestContext context, AccessorFactory accessorFactory, ResolverFactory resolverFactory) {
        this.accessor = accessorFactory.getPlugin(context);
        this.resolver = resolverFactory.getPlugin(context);
    }

    @Override
    public boolean isThreadSafe() {
        boolean result = accessor.isThreadSafe() && resolver.isThreadSafe();
        LOG.debug("Bridge is " + (result ? "" : "not ") + "thread safe");
        return result;
    }
}
