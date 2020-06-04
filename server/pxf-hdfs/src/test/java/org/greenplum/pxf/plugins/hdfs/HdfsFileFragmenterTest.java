package org.greenplum.pxf.plugins.hdfs;

import org.apache.hadoop.mapred.InvalidInputException;
import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.Fragmenter;
import org.greenplum.pxf.api.model.RequestContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;
import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Test the HdfsFileFragmenter
 */
public class HdfsFileFragmenterTest {
    private String csvResourcePath;
    private RequestContext context;
    private Fragmenter fragmenter;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() {
        csvResourcePath = Objects.requireNonNull(this.getClass().getClassLoader().getResource("csv/")).getPath();
        context = new RequestContext();
        context.setConfig("default");
        context.setUser("test-user");
        context.setProfileScheme("localfile");
        fragmenter = new HdfsFileFragmenter();
    }

    @Test
    public void testFragmenterErrorsWhenPathDoesNotExist() throws Exception {
        expectedException.expect(InvalidInputException.class);
        expectedException.expectMessage("Input path does not exist:");
        context.setDataSource(csvResourcePath + "non-existent");
        fragmenter.initialize(context);
        fragmenter.getFragments();
    }

    @Test
    public void testFragmenterReturnsListOfFiles() throws Exception {
        context.setDataSource(csvResourcePath);
        fragmenter.initialize(context);
        List<Fragment> fragmentList = fragmenter.getFragments();

        assertNotNull(fragmentList);
        assertEquals(4, fragmentList.size());
    }

    @Test
    public void testFragmenterWilcardPath() throws Exception {
        context.setDataSource(csvResourcePath + "*.csv");
        fragmenter.initialize(context);
        List<Fragment> fragmentList = fragmenter.getFragments();

        assertNotNull(fragmentList);
        assertEquals(4, fragmentList.size());
    }

    @Test
    public void testInvalidInputPath() throws Exception {
        expectedException.expect(InvalidInputException.class);
        expectedException.expectMessage("Input Pattern file:/tmp/non-existent-path-on-disk/*.csv matches 0 files");

        RequestContext context = new RequestContext();
        context.setConfig("default");
        context.setUser("test-user");
        context.setProfileScheme("localfile");
        context.setDataSource("/tmp/non-existent-path-on-disk/*.csv");

        Fragmenter fragmenter = new HdfsFileFragmenter();
        fragmenter.initialize(context);
        fragmenter.getFragments();
    }

    @Test
    public void testInvalidInputPathIgnored() throws Exception {
        RequestContext context = new RequestContext();
        context.setConfig("default");
        context.setUser("test-user");
        context.setProfileScheme("localfile");
        context.addOption("IGNORE_MISSING_PATH", "true");
        context.setDataSource("/tmp/non-existent-path-on-disk/*.csv");

        Fragmenter fragmenter = new HdfsFileFragmenter();
        fragmenter.initialize(context);

        List<Fragment> fragmentList = fragmenter.getFragments();
        assertNotNull(fragmentList);
        assertEquals(0, fragmentList.size());
    }
}
