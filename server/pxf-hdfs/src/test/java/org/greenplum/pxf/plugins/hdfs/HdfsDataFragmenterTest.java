package org.greenplum.pxf.plugins.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InvalidInputException;
import org.greenplum.pxf.api.model.ConfigurationFactory;
import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.Fragmenter;
import org.greenplum.pxf.api.model.RequestContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HdfsDataFragmenterTest {

    private Fragmenter fragmenter;
    private RequestContext context;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() {

        Configuration configuration = new Configuration();
        configuration.set("pxf.fs.basePath", "/");

        context = new RequestContext();
        context.setConfig("default");
        context.setServerName("default");
        context.setUser("test-user");

        ConfigurationFactory mockConfigurationFactory = mock(ConfigurationFactory.class);
        when(mockConfigurationFactory.
                initConfiguration(context.getConfig(), context.getServerName(), context.getUser(), context.getAdditionalConfigProps()))
                .thenReturn(configuration);

        fragmenter = new HdfsDataFragmenter(mockConfigurationFactory);
    }

    @Test
    public void testFragmenterReturnsListOfFiles() throws Exception {
        String path = this.getClass().getClassLoader().getResource("csv/").getPath();
        context.setDataSource(path);

        fragmenter.initialize(context);

        List<Fragment> fragmentList = fragmenter.getFragments();
        assertNotNull(fragmentList);
        // empty.csv gets ignored
        assertEquals(8, fragmentList.size());
    }

    @Test
    public void testFragmenterWilcardPath() throws Exception {
        String path = this.getClass().getClassLoader().getResource("csv/").getPath();
        context.setDataSource(path + "*.csv");

        fragmenter.initialize(context);

        List<Fragment> fragmentList = fragmenter.getFragments();
        assertNotNull(fragmentList);
        // empty.csv gets ignored
        assertEquals(8, fragmentList.size());
    }

    @Test
    public void testInvalidInputPath() throws Exception {
        thrown.expect(InvalidInputException.class);
        thrown.expectMessage("Input Pattern file:/tmp/non-existent-path-on-disk/*.csv matches 0 files");

        context.setDataSource("/tmp/non-existent-path-on-disk/*.csv");

        fragmenter.initialize(context);
        fragmenter.getFragments();
    }

    @Test
    public void testInvalidInputPathIgnored() throws Exception {
        context.addOption("IGNORE_MISSING_PATH", "true");
        context.setDataSource("/tmp/non-existent-path-on-disk/*.csv");

        fragmenter.initialize(context);

        List<Fragment> fragmentList = fragmenter.getFragments();
        assertNotNull(fragmentList);
        assertEquals(0, fragmentList.size());
    }
}