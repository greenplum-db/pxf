package org.greenplum.pxf.service.bridge;

import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.Resolver;
import org.greenplum.pxf.service.utilities.BasePluginFactory;
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
    protected RequestContext context;

    public BaseBridge(BasePluginFactory pluginFactory, RequestContext context) {
        String accessorClassName = context.getAccessor();
        String resolverClassName = context.getResolver();

        LOG.debug("Creating accessor bean '{}' and resolver bean '{}'", accessorClassName, resolverClassName);

        this.context = context;
        this.accessor = pluginFactory.getPlugin(context, accessorClassName);
        this.resolver = pluginFactory.getPlugin(context, resolverClassName);
    }
}
