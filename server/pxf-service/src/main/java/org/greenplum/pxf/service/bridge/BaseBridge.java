package org.greenplum.pxf.service.bridge;

import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.Resolver;
import org.greenplum.pxf.api.utilities.Utilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

/**
 * Abstract class representing the bridge that provides to subclasses logger and accessor and
 * resolver instances obtained from the factories.
 */
public abstract class BaseBridge implements Bridge {

    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    protected Accessor accessor;
    protected Resolver resolver;

    public BaseBridge(ApplicationContext applicationContext, RequestContext context) {
        String accessorClassName = Utilities.getShortClassName(context.getAccessor());
        String resolverClassName = Utilities.getShortClassName(context.getResolver());

        LOG.debug("Creating accessor bean '{}' and resolver bean '{}'", accessorClassName, resolverClassName);

        this.accessor = applicationContext.getBean(accessorClassName, Accessor.class);
        this.resolver = applicationContext.getBean(resolverClassName, Resolver.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isThreadSafe() {
        boolean result = accessor.isThreadSafe() && resolver.isThreadSafe();
        LOG.debug("Bridge is {}thread safe", (result ? "" : "not "));
        return result;
    }
}
