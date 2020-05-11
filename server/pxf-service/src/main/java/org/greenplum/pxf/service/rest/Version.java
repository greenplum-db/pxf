package org.greenplum.pxf.service.rest;

/**
 * PXF protocol version. Any call to PXF resources should include the current
 * version e.g. {@code ...pxf/v15/Bridge}
 */
public class Version {
    /**
     * Constant which holds current protocol version. Getting replaced with
     * actual value on build stage, using pxfProtocolVersion parameter from
     * gradle.properties
     */
    public final static String PXF_PROTOCOL_VERSION = "v15";
}
