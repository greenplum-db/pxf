package org.greenplum.pxf.api.model;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.FileFilter;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

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
    private Map<String, String> options;
    private File mockServersDirectory;
    private File serversDirectory;

    @Before
    public void setup() throws URISyntaxException {
        mockServersDirectory = mock(File.class);
        options = new HashMap<>();
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

        factory.initConfiguration("dummy", null);
    }

    @Test
    public void testConfigurationsLoadedFromMultipleFilesForDefaultServer() {
        Configuration configuration = factory.initConfiguration("default", null);

        assertEquals("blue", configuration.get("test.blue"));
        assertEquals("red", configuration.get("test.red"));

        // Should return null because the file name does not end in -site.xml
        assertNull(configuration.get("test.green"));
    }

    @Test
    public void testConfigurationsLoadedForCaseInsensitiveServerName() {
        Configuration configuration = factory.initConfiguration("DeFAulT", null);

        assertEquals("blue", configuration.get("test.blue"));
        assertEquals("red", configuration.get("test.red"));

        // Should return null because the file name does not end in -site.xml
        assertNull(configuration.get("test.green"));
    }

    @Test
    public void testConfigurationsLoadedAndOptionsAdded() {
        options.put("test.newOption", "newOption");
        options.put("test.red", "purple");
        Configuration configuration = factory.initConfiguration("default", options);

        assertEquals("blue", configuration.get("test.blue"));
        assertEquals("purple", configuration.get("test.red"));
        assertEquals("newOption", configuration.get("test.newOption"));

        // Should return null because the file name does not end in -site.xml
        assertNull(configuration.get("test.green"));
    }

    @Test
    public void testConfigurationsNotLoadedForUnknownServer() {
        options.put("test.newOption", "newOption");
        options.put("test.red", "purple");
        Configuration configuration = factory.initConfiguration("unknown", options);

        assertNull(configuration.get("test.blue"));
        assertEquals("purple", configuration.get("test.red"));
        assertEquals("newOption", configuration.get("test.newOption"));

        // Should return null because the file name does not end in -site.xml
        assertNull(configuration.get("test.green"));
    }

}
