package org.greenplum.pxf.service;

import org.apache.coyote.ProtocolHandler;
import org.apache.coyote.http11.AbstractHttp11Protocol;
import org.greenplum.pxf.api.configuration.PxfServerProperties;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.stereotype.Component;

/**
 * The {@link PxfCustomContainer} class allows customizing application container
 * properties that are not exposed through the application.properties file.
 * For example, setting the max header count or the http header size.
 */
@Component
public class PxfCustomContainer implements
        WebServerFactoryCustomizer<TomcatServletWebServerFactory> {

    private final PxfServerProperties serverProperties;

    /**
     * Create a new PxfCustomContainer with the given server properties
     *
     * @param serverProperties the server properties
     */
    public PxfCustomContainer(PxfServerProperties serverProperties) {
        this.serverProperties = serverProperties;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void customize(TomcatServletWebServerFactory factory) {
        factory.addConnectorCustomizers(connector -> {
            ProtocolHandler handler = connector.getProtocolHandler();
            if (handler instanceof AbstractHttp11Protocol) {
                AbstractHttp11Protocol protocol = (AbstractHttp11Protocol) handler;
                protocol.setMaxHeaderCount(serverProperties.getTomcat().getMaxHeaderCount());
            }
        });
    }
}
