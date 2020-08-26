package org.greenplum.pxf.service.utilities;

import org.greenplum.pxf.api.model.Plugin;
import org.greenplum.pxf.api.model.RequestContext;

/**
 * Factory interface for getting instances of the plugins based on their class names.
 *
 * @param <T> interface that the resulting plugin should implement
 */
public interface PluginFactory {

    /**
     * Get an instance of the plugin with a given class names.
     *
     * @param context         context of the current request
     * @param pluginClassName the fully qualified name of the class
     * @return an initialized instance of the plugin
     */
    <T extends Plugin> T getPlugin(RequestContext context, String pluginClassName);
}
