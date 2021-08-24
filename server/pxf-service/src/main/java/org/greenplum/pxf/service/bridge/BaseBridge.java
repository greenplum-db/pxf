package org.greenplum.pxf.service.bridge;

import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.Resolver;
import org.greenplum.pxf.service.utilities.BasePluginFactory;
import org.greenplum.pxf.service.utilities.GSSFailureHandler;
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
    protected BasePluginFactory pluginFactory;
    protected RequestContext context;
    protected GSSFailureHandler failureHandler;

    /**
     * Creates a new instance of the bridge.
     *
     * @param pluginFactory plugin factory
     * @param context request context
     * @param failureHandler failure handler
     */
    public BaseBridge(BasePluginFactory pluginFactory, RequestContext context, GSSFailureHandler failureHandler) {
        this.pluginFactory = pluginFactory;
        this.context = context;
        this.failureHandler = failureHandler;

        String accessorClassName = context.getAccessor();
        String resolverClassName = context.getResolver();
        LOG.debug("Creating accessor '{}' and resolver '{}'", accessorClassName, resolverClassName);

        this.accessor = pluginFactory.getPlugin(context, accessorClassName);
        this.resolver = pluginFactory.getPlugin(context, resolverClassName);
    }

    /**
     * A function that is called by the failure handler before a new retry attempt after a failure.
     * It re-creates the accessor from the factory in case the accessor implementation is not idempotent.
     */
    protected void beforeRetryCallback() {
        String accessorClassName = context.getAccessor();
        LOG.debug("Creating accessor '{}'", accessorClassName);
        this.accessor = pluginFactory.getPlugin(context, accessorClassName);
    }
}
