package org.greenplum.pxf.api.model;

import java.util.Map;

public interface Profile {

    String getName();

    Map<String, String> getPlugins();

    String getProtocol();

}
