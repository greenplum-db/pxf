package org.greenplum.pxf.api.model;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import static org.greenplum.pxf.api.model.ConfigurationFactory.PXF_CONFIG_RESOURCE_PATH_PROPERTY;
import static org.greenplum.pxf.api.model.ConfigurationFactory.PXF_CONFIG_SERVER_DIRECTORY_PROPERTY;
import static org.greenplum.pxf.api.model.ConfigurationFactory.PXF_SESSION_USER_PROPERTY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BaseConfigurationFactoryTest {

    private BaseConfigurationFactory factory;
    private Map<String, String> additionalProperties;
    private File mockServersDirectory;
    private File serversDirectory;

    @BeforeEach
    public void setup() throws URISyntaxException {
        mockServersDirectory = mock(File.class);
        additionalProperties = new HashMap<>();
        serversDirectory = new File(this.getClass().getClassLoader().getResource("servers").toURI());
        factory = new BaseConfigurationFactory(serversDirectory);
    }

//    @Test
//    public void testGetInstance() {
//        assertSame(BaseConfigurationFactory.getInstance(), BaseConfigurationFactory.getInstance());
//    }

    @Test
    public void testInitConfigurationFailsWhenMultipleDirectoriesWithSameName() {
        Exception ex = assertThrows(
                IllegalStateException.class,
                () -> {
                    factory = new BaseConfigurationFactory(mockServersDirectory);
                    when(mockServersDirectory.listFiles(any(FileFilter.class))).thenReturn(new File[]{new File("a"), new File("b")});

                    factory.initConfiguration("dummy", "dummy", "dummy", null);
                });

        assertEquals("Multiple directories found for server dummy. Server directories are expected to be case-insensitive.", ex.getMessage());
    }

    @Test
    public void testConfigurationsLoadedFromMultipleFilesForDefaultServer() {
        Configuration configuration = factory.initConfiguration("default", "default", "dummy", null);

        assertEquals("blue", configuration.get("test.blue"));
        assertEquals("red", configuration.get("test.red"));

        // Should return null because the file name does not end in -site.xml
        assertNull(configuration.get("test.green"));

        assertEquals("bluevaluefromuser", configuration.get("test.blue.key"));
        assertEquals("redvaluefromuser", configuration.get("test.red.key"));
        assertEquals("uservalue", configuration.get("test.user.key"));
    }

    @Test
    public void testConfigurationsLoadedWithInterpolationFromMultipleFilesForDefaultServer() {
        Configuration configuration = factory.initConfiguration("default", "default", "dummy", null);

        assertEquals("blue", configuration.get("test.blue"));
        assertEquals("dummy-blue", configuration.get("test.blue.interpolated.key"));
        assertEquals("red", configuration.get("test.red"));
        assertEquals("dummy-red", configuration.get("test.red.interpolated.key"));

        // Should return null because the file name does not end in -site.xml
        assertNull(configuration.get("test.green"));

        assertEquals("bluevaluefromuser", configuration.get("test.blue.key"));
        assertEquals("redvaluefromuser", configuration.get("test.red.key"));
        assertEquals("uservalue", configuration.get("test.user.key"));
        assertEquals("dummy-user", configuration.get("test.user.interpolated.key"));

    }

    @Test
    public void testConfigurationsLoadedFromMultipleFilesForDefaultServerWithCustomName() {
        Configuration configuration = factory.initConfiguration("default", "my-fancy-server-name", "dummy", null);

        assertEquals("blue", configuration.get("test.blue"));
        assertEquals("red", configuration.get("test.red"));

        // Should return null because the file name does not end in -site.xml
        assertNull(configuration.get("test.green"));

        assertEquals("bluevaluefromuser", configuration.get("test.blue.key"));
        assertEquals("redvaluefromuser", configuration.get("test.red.key"));
        assertEquals("uservalue", configuration.get("test.user.key"));
    }

    @Test
    public void testConfigurationsLoadedFromMultipleFilesForDefaultServerWithAbsolutePath() {
        String config = this.getClass().getClassLoader().getResource("servers/default").getPath();
        Configuration configuration = factory.initConfiguration(config, "default", "dummy", null);

        assertEquals("blue", configuration.get("test.blue"));
        assertEquals("red", configuration.get("test.red"));

        // Should return null because the file name does not end in -site.xml
        assertNull(configuration.get("test.green"));

        assertEquals("bluevaluefromuser", configuration.get("test.blue.key"));
        assertEquals("redvaluefromuser", configuration.get("test.red.key"));
        assertEquals("uservalue", configuration.get("test.user.key"));
    }

    @Test
    public void testConfigurationsLoadedForCaseInsensitiveServerName() {
        Configuration configuration = factory.initConfiguration("DeFAulT", "DeFAulT", "dummy", null);

        assertEquals("blue", configuration.get("test.blue"));
        assertEquals("red", configuration.get("test.red"));

        // Should return null because the file name does not end in -site.xml
        assertNull(configuration.get("test.green"));

        assertEquals("bluevaluefromuser", configuration.get("test.blue.key"));
        assertEquals("redvaluefromuser", configuration.get("test.red.key"));
        assertEquals("uservalue", configuration.get("test.user.key"));
    }

    @Test
    public void testConfigurationsLoadedAndOptionsAdded() {
        additionalProperties.put("test.newOption", "newOption");
        additionalProperties.put("test.red", "purple");
        additionalProperties.put("test.blue.key", "bluevaluechanged");
        additionalProperties.put("test.user.key", "uservaluechanged");
        Configuration configuration = factory.initConfiguration("default", "default", "dummy", additionalProperties);

        assertEquals("blue", configuration.get("test.blue"));
        assertEquals("purple", configuration.get("test.red"));
        assertEquals("newOption", configuration.get("test.newOption"));

        // Should return null because the file name does not end in -site.xml
        assertNull(configuration.get("test.green"));

        assertEquals("bluevaluefromuser", configuration.get("test.blue.key"));
        assertEquals("redvaluefromuser", configuration.get("test.red.key"));
        assertEquals("uservalue", configuration.get("test.user.key"));

    }

    @Test
    public void testConfigurationsNotLoadedForUnknownServer() {
        additionalProperties.put("test.newOption", "newOption");
        additionalProperties.put("test.red", "purple");
        Configuration configuration = factory.initConfiguration("unknown", "unknown", "dummy", additionalProperties);

        assertNull(configuration.get("test.blue"));
        assertNull(configuration.get("test.blue.key"));
        assertNull(configuration.get("test.red.key"));
        assertNull(configuration.get("test.user.key"));
        assertEquals("purple", configuration.get("test.red"));
        assertEquals("newOption", configuration.get("test.newOption"));

        // Should return null because the file name does not end in -site.xml
        assertNull(configuration.get("test.green"));
    }

    @Test
    public void testConfigurationSetsResourcePath() throws MalformedURLException {
        Configuration configuration = factory.initConfiguration("default", "default", "dummy", additionalProperties);
        File defaultServerDirectory = new File(serversDirectory, "default");

        assertEquals(new File(defaultServerDirectory, "test-blue-site.xml").toPath().toUri().toURL().toString(),
                configuration.get(String.format("%s.%s", PXF_CONFIG_RESOURCE_PATH_PROPERTY, "test-blue-site.xml")));
        assertEquals(new File(defaultServerDirectory, "test-red-site.xml").toPath().toUri().toURL().toString(),
                configuration.get(String.format("%s.%s", PXF_CONFIG_RESOURCE_PATH_PROPERTY, "test-red-site.xml")));
        assertEquals(new File(defaultServerDirectory, "dummy-user.xml").toPath().toUri().toURL().toString(),
                configuration.get(String.format("%s.%s", PXF_CONFIG_RESOURCE_PATH_PROPERTY, "dummy-user.xml")));
    }

    @Test
    public void testConfigurationSetsServerDirectoryPath() throws IOException {
        Configuration configuration = factory.initConfiguration("default", "default", "dummy", additionalProperties);
        File defaultServerDirectory = new File(serversDirectory, "default");

        assertEquals(defaultServerDirectory.getCanonicalPath(), configuration.get(PXF_CONFIG_SERVER_DIRECTORY_PROPERTY));
    }

    @Test
    public void testConfigurationSetsSessionUser() throws IOException {
        Configuration configuration = factory.initConfiguration("default", "default", "dummy", additionalProperties);
        File defaultServerDirectory = new File(serversDirectory, "default");

        assertEquals("dummy", configuration.get(PXF_SESSION_USER_PROPERTY));
    }

}
