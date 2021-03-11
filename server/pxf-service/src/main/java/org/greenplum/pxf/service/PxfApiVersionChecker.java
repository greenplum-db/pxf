package org.greenplum.pxf.service;

import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Component;

/**
 * Checks if two API versions are compatible
 */
@Component
public class PxfApiVersionChecker {

    /**
     * @param serverApiVersion
     * @param clientApiVersion
     * @return true if the server is compatible with the client's API version
     */
    public boolean isCompatible(String serverApiVersion, String clientApiVersion) {
        return StringUtils.equalsIgnoreCase(serverApiVersion, clientApiVersion);
    }
}
