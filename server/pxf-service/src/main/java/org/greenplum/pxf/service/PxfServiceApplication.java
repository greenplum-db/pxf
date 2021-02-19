package org.greenplum.pxf.service;

import org.apache.hadoop.security.PxfUserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.net.URL;
import java.net.URLClassLoader;

/**
 * Main PXF Spring Configuration class.
 */
@SpringBootApplication(scanBasePackages = "org.greenplum.pxf", scanBasePackageClasses = PxfUserGroupInformation.class)
public class PxfServiceApplication {

    private static final Logger LOG = LoggerFactory.getLogger(PxfServiceApplication.class);

    /**
     * Constructs a new PxfServiceApplication
     */
    public PxfServiceApplication() {
        logClassLoaderInfo();
    }

    /**
     * Logs, at info level, all the libraries loaded by the ClassLoader used by
     * the PxfServiceApplication.
     */
    private void logClassLoaderInfo() {
        ClassLoader loader = this.getClass().getClassLoader();
        if (loader instanceof URLClassLoader) {
            URLClassLoader urlClassLoader = (URLClassLoader) loader;
            URL[] urls = urlClassLoader.getURLs();
            if (urls != null) {
                for (URL url : urls) {
                    LOG.info("Added repository {}", url);
                }
            }
        }
    }

    /**
     * Spring Boot Main.
     *
     * @param args program arguments
     */
    public static void main(String[] args) {
        SpringApplication.run(PxfServiceApplication.class, args);
    }

}
