package org.greenplum.pxf.api.configuration;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.autoconfigure.task.TaskExecutionProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Pattern;

/**
 * Configuration properties for PXF.
 */
@ConfigurationProperties(prefix = PxfServerProperties.PROPERTY_PREFIX)
@Validated
public class PxfServerProperties {

    public static final String PXF_CONF_PROPERTY = "pxf.conf";

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
    @Getter
    private String conf;

    /**
     * Enable caching of metadata calls from a single JVM
     */
    @Getter
    @Setter
    private boolean metadataCacheEnabled = true;

    /**
     * Customizable settings for tomcat through PXF
     */
    @Getter
    @Setter
    private Tomcat tomcat = new Tomcat();

    /**
     * Configurable task execution properties for async tasks (i.e Bridge Read)
     */
    @Getter
    @Setter
    private TaskExecutionProperties task = new TaskExecutionProperties();

    @Getter
    @Setter
    public static class Tomcat {

        /**
         * Maximum number of headers allowed in the request
         */
        private int maxHeaderCount = 30000;
    }

    public void setConf(String conf) {
        this.conf = conf;
        System.setProperty(PXF_CONF_PROPERTY, conf);
    }
}
