package org.greenplum.pxf.api.model;

import java.util.Map;

public class BaseProfile implements Profile {

    private final String name;
    private final String protocol;
    private final Map<String, String> plugins;
    private final Map<String, String> whitelist;

    public BaseProfile(
            String name,
            String protocol,
            Map<String, String> plugins,
            Map<String, String> whitelist) {
        this.name = name;
        this.protocol = protocol;
        this.plugins = plugins;
        this.whitelist = whitelist;
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

    @Override
    public Map<String, String> getWhitelistMap() {
        return whitelist;
    }
}
