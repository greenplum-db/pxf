package org.greenplum.pxf.api.model;

import java.util.Map;

public class BaseProfile implements Profile {

    private final String name;
    private final String protocol;
    private final Map<String, String> plugins;

    public BaseProfile(String name, String protocol, Map<String, String> plugins) {
        this.name = name;
        this.protocol = protocol;
        this.plugins = plugins;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Map<String, String> getPlugins() {
        return plugins;
    }

    @Override
    public String getProtocol() {
        return protocol;
    }
}
