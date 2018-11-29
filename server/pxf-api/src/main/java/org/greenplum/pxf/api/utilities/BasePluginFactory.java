package org.greenplum.pxf.api.utilities;

import org.apache.commons.lang.StringUtils;
import org.greenplum.pxf.api.model.Plugin;
import org.greenplum.pxf.api.model.RequestContext;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public abstract class BasePluginFactory<T extends Plugin> implements PluginFactory<T> {

    @Override
    public T getPlugin(RequestContext requestContext) {

        // get the class name of the plugin
        String pluginClassName = getPluginClassName(requestContext);
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
        if (! Plugin.class.isAssignableFrom(cls)) {
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
        instance.initialize(requestContext);

        // cast into a target type
        @SuppressWarnings("unchecked")
        T castInstance = (T) instance;

        return castInstance;
    }

    abstract protected String getPluginClassName(RequestContext requestContext);
}
