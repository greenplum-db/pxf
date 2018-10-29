package org.greenplum.pxf.plugins.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static org.junit.Assert.*;

public class ConfigurationCacheTest {

    @Test
    public void returnsNewConfiguration() {
        Configuration configuration = ConfigurationCache.getConfiguration("default");
        assertNotNull(configuration);
    }

    @Test
    public void returnSameConfigurationForSameServer() {
        Configuration configuration1 = ConfigurationCache.getConfiguration("default");
        Configuration configuration2 = ConfigurationCache.getConfiguration("default");

        assertEquals(configuration1, configuration2);
    }

    @Test
    public void differentConfigurationForDifferentServers() {
        Configuration defaultConfiguration = ConfigurationCache.getConfiguration("default");
        Configuration server1Configuration = ConfigurationCache.getConfiguration("server1");

        assertNotEquals(defaultConfiguration, server1Configuration);
    }

}