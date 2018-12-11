package org.greenplum.pxf.api.model;

import java.util.Map;

public interface Profile {

    String getName();

    Map<String, String> getPluginTable();

    String getProtocol();

    Map<String, String> getOptionMappingTable();

}
