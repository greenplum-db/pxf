package org.greenplum.pxf.service.bridge;

import org.apache.commons.lang.StringUtils;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.Plugin;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.Resolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * Abstract class representing the bridge that provides to subclasses logger and accessor and
 * resolver instances obtained from the factories.
 */
public abstract class BaseBridge implements Bridge {

    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    protected Accessor accessor;
    protected Resolver resolver;
    protected RequestContext context;

    public BaseBridge(RequestContext context) {
        String accessorClassName = context.getAccessor();
        String resolverClassName = context.getResolver();

        LOG.debug("Creating accessor bean '{}' and resolver bean '{}'", accessorClassName, resolverClassName);

        this.context = context;
        this.accessor = getPlugin(context, accessorClassName);
        this.resolver = getPlugin(context, resolverClassName);
    }

    public <T> T getPlugin(RequestContext context, String pluginClassName) {

        // get the class name of the plugin
        if (StringUtils.isBlank(pluginClassName)) {
            throw new RuntimeException("Could not determine plugin class name");
        }

        // load the class by name
        Class<?> cls;
        try {
            cls = Class.forName(pluginClassName);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(String.format("Class %s is not found", pluginClassName), e);
        }

        // check if the class is a plugin
        if (!Plugin.class.isAssignableFrom(cls)) {
            throw new RuntimeException(String.format("Class %s does not implement Plugin interface", pluginClassName));
        }

        // get the empty constructor
        Constructor<?> con;
        try {
            con = cls.getConstructor();
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(String.format("Class %s does not have an empty constructor", pluginClassName));
        }

        // create plugin instance
        Plugin instance;
        try {
            instance = (Plugin) con.newInstance();
        } catch (InvocationTargetException e) {
            throw (e.getCause() != null) ? new RuntimeException(e.getCause()) :
                    new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Class %s could not be instantiated", pluginClassName), e);
        }

        // initialize the instance
        instance.setRequestContext(context);
        instance.afterPropertiesSet();

        // cast into a target type
        @SuppressWarnings("unchecked")
        T castInstance = (T) instance;

        return castInstance;
    }
}
