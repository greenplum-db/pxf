package org.greenplum.pxf.api.model;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import static org.greenplum.pxf.api.model.ConfigurationFactory.PXF_CONFIG_RESOURCE_PATH_PROPERTY;
import static org.greenplum.pxf.api.model.ConfigurationFactory.PXF_CONFIG_SERVER_DIRECTORY_PROPERTY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

public class BaseConfigurationFactoryTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private BaseConfigurationFactory factory;
    private Map<String, String> additionalProperties;
    private File mockServersDirectory;
    private File serversDirectory;

    @Before
    public void setup() throws URISyntaxException {
        mockServersDirectory = mock(File.class);
        additionalProperties = new HashMap<>();
        serversDirectory = new File(this.getClass().getClassLoader().getResource("servers").toURI());
        factory = new BaseConfigurationFactory(serversDirectory);
    }

    @Test
    public void testGetInstance() {
        assertSame(BaseConfigurationFactory.getInstance(), BaseConfigurationFactory.getInstance());
    }

    @Test
    public void testInitConfigurationFailsWhenMultipleDirectoriesWithSameName() {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("Multiple directories found for server dummy. Server directories are expected to be case-insensitive.");

        factory = new BaseConfigurationFactory(mockServersDirectory);
        when(mockServersDirectory.listFiles(any(FileFilter.class))).thenReturn(new File[]{new File("a"), new File("b")});

        factory.initConfiguration("dummy", "dummy", null);
    }

    @Test
    public void testConfigurationsLoadedFromMultipleFilesForDefaultServer() {
        Configuration configuration = factory.initConfiguration("default", "dummy", null);

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
        Configuration configuration = factory.initConfiguration("DeFAulT", "dummy", null);

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
        Configuration configuration = factory.initConfiguration("default", "dummy", additionalProperties);

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
        Configuration configuration = factory.initConfiguration("unknown", "dummy", additionalProperties);

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
        Configuration configuration = factory.initConfiguration("default", "dummy", additionalProperties);
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
        Configuration configuration = factory.initConfiguration("default", "dummy", additionalProperties);
        File defaultServerDirectory = new File(serversDirectory, "default");

        assertEquals(defaultServerDirectory.getCanonicalPath(), configuration.get(PXF_CONFIG_SERVER_DIRECTORY_PROPERTY));
    }

}
