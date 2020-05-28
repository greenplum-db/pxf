package org.greenplum.pxf.api.configuration;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Pattern;

@ConfigurationProperties(prefix = PxfServerProperties.PROPERTY_PREFIX)
@Getter
@Setter
@Validated
public class PxfServerProperties {

    /**
     * The property prefix for all properties in this group.
     */
    public static final String PROPERTY_PREFIX = "pxf";

    /**
     * The path for the server configuration. If the configuration has not
     * been initialized, it will be set to NOT_INITIALIZED. This will cause
     * the application to fail during start up.
     */
    @NotBlank
    @Pattern(regexp = "^(?!NOT_INITIALIZED).*$")
    private String conf;

    /**
     * Enable caching of metadata calls from a single JVM
     */
    private boolean metadataCacheEnabled = true;

    /**
     * Customizable settings for tomcat through PXF
     */
    private Tomcat tomcat = new Tomcat();

    @Getter
    @Setter
    public static class Tomcat {

        /**
         * Maximum number of headers allowed in the request
         */
        private int maxHeaderCount = 30000;
    }

}
