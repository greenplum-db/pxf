package org.greenplum.pxf.service;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Main PXF Spring Configuration class.
 */
@SpringBootApplication(scanBasePackages = "org.greenplum.pxf")
public class PxfServiceApplication {

    /**
     * Spring Boot Main.
     *
     * @param args program arguments
     */
    public static void main(String[] args) {
        SpringApplication.run(PxfServiceApplication.class, args);
    }

}
