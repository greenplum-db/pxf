package org.greenplum.pxf.service.bridge;

import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.Resolver;
import org.greenplum.pxf.api.utilities.AccessorFactory;
import org.greenplum.pxf.api.utilities.ResolverFactory;
import org.greenplum.pxf.service.utilities.GSSFailureHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract class representing the bridge that provides to subclasses logger and accessor and
 * resolver instances obtained from the factories.
 */
public abstract class BaseBridge implements Bridge {

    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    protected RequestContext context;
    protected AccessorFactory accessorFactory;
    protected Accessor accessor;
    protected Resolver resolver;
    protected GSSFailureHandler failureHandler;


    /**
     * Creates a new instance for a given request context. Uses default singleton instances of
     * plugin factories to request accessor and resolver.
     * @param context request context
     */
    public BaseBridge(RequestContext context) {
        this(context, AccessorFactory.getInstance(), ResolverFactory.getInstance(), GSSFailureHandler.getInstance());
    }

    /**
     * Creates a new instance for a given request context. Uses provides instances of
     * plugin factories to request accessor and resolver.
     * @param context request context
     * @param accessorFactory accessor factory
     * @param resolverFactory resolver factory
     * @param failureHandler gss failure handler
     */
    BaseBridge(RequestContext context, AccessorFactory accessorFactory, ResolverFactory resolverFactory, GSSFailureHandler failureHandler) {
        this.context = context;
        this.accessorFactory = accessorFactory;
        this.accessor = accessorFactory.getPlugin(context);
        this.resolver = resolverFactory.getPlugin(context);
        this.failureHandler = failureHandler;
    }

    /**
     * A function that is called by the failure handler before a new retry attempt after a failure.
     * It re-creates the accessor from the factory in case the accessor implementation is not idempotent.
     */
    protected void beforeRetryCallback() {
        accessor = accessorFactory.getPlugin(context);
    }
}
