package org.greenplum.pxf.service.spring;

import org.apache.coyote.ProtocolHandler;
import org.apache.coyote.http11.AbstractHttp11Protocol;
import org.greenplum.pxf.api.configuration.PxfServerProperties;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.stereotype.Component;

/**
 * The {@link PxfTomcatCustomizer} class allows customizing application container
 * properties that are not exposed through the application.properties file.
 * For example, setting the max header count or the http header size.
 */
@Component
public class PxfTomcatCustomizer implements
        WebServerFactoryCustomizer<TomcatServletWebServerFactory> {

    private final PxfServerProperties serverProperties;

    /**
     * Create a new PxfCustomContainer with the given server properties
     *
     * @param serverProperties the server properties
     */
    public PxfTomcatCustomizer(PxfServerProperties serverProperties) {
        this.serverProperties = serverProperties;
    }

    @Override
    public void customize(TomcatServletWebServerFactory factory) {
        factory.addConnectorCustomizers(connector -> {
            ProtocolHandler handler = connector.getProtocolHandler();
            if (handler instanceof AbstractHttp11Protocol) {
                AbstractHttp11Protocol<?> protocolHandler = (AbstractHttp11Protocol<?>) handler;
                protocolHandler.setMaxHeaderCount(serverProperties.getTomcat().getMaxHeaderCount());
                protocolHandler.setDisableUploadTimeout(serverProperties.getTomcat().isDisableUploadTimeout());
                protocolHandler.setConnectionUploadTimeout((int) serverProperties.getTomcat().getConnectionUploadTimeout().toMillis());
            }
        });
    }
}
