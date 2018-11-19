package org.greenplum.pxf.api.model;

public interface ConfigurationFactory<T> {

    T getConfiguration(RequestContext requestContext);
}
