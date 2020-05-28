package org.greenplum.pxf.service.rest;

import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.model.ConfigurationFactory;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.service.RequestParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.MultiValueMap;

/**
 * Base abstract implementation of the resource class, provides logger and request parser
 * to the subclasses.
 */
public abstract class BaseResource {

    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());
    protected final RequestContext.RequestType requestType;

    private ConfigurationFactory configurationFactory;
    private RequestParser<MultiValueMap<String, String>> parser;

    /**
     * Creates an instance of the resource with a given request parser.
     *
     * @param requestType the type of the request
     */
    BaseResource(RequestContext.RequestType requestType) {
        this.requestType = requestType;
    }

    /**
     * Sets the {@link ConfigurationFactory}. In the spring boot context,
     * the {@link ConfigurationFactory} is injected automatically.
     *
     * @param configurationFactory the configuration factory
     */
    @Autowired
    public void setConfigurationFactory(ConfigurationFactory configurationFactory) {
        this.configurationFactory = configurationFactory;
    }

    /**
     * Sets the {@link RequestParser<MultiValueMap<String, String>>} in charge
     * of parsing the request headers. In the spring boot context, this value
     * is injected automatically.
     *
     * @param parser the request parser
     */
    @Autowired
    public void setRequestParser(RequestParser<MultiValueMap<String, String>> parser) {
        this.parser = parser;
    }

    /**
     * Parses incoming request into request context
     *
     * @param headers the HTTP headers of incoming request
     * @return parsed request context
     */
    protected RequestContext parseRequest(MultiValueMap<String, String> headers) {
        RequestContext context = parser.parseRequest(headers, requestType);

        // Initialize the configuration for this request
        Configuration configuration = configurationFactory.
                initConfiguration(
                        context.getConfig(),
                        context.getServerName(),
                        context.getUser(),
                        context.getAdditionalConfigProps());

        context.setConfiguration(configuration);

        return context;
    }
}
