package org.greenplum.pxf.plugins.hdfs;

import org.apache.hadoop.mapred.InvalidInputException;
import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.Fragmenter;
import org.greenplum.pxf.api.model.RequestContext;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class HdfsDataFragmenterTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testFragmenterReturnsListOfFiles() throws Exception {
        String path = this.getClass().getClassLoader().getResource("csv/").getPath();

        RequestContext context = new RequestContext();
        context.setConfig("default");
        context.setUser("test-user");
        context.setProfileScheme("localfile");
        context.setDataSource(path);

        Fragmenter fragmenter = new HdfsDataFragmenter();
        fragmenter.initialize(context);

        List<Fragment> fragmentList = fragmenter.getFragments();
        assertNotNull(fragmentList);
        // empty.csv gets ignored
        assertEquals(3, fragmentList.size());
    }

    @Test
    public void testFragmenterWilcardPath() throws Exception {
        String path = this.getClass().getClassLoader().getResource("csv/").getPath();

        RequestContext context = new RequestContext();
        context.setConfig("default");
        context.setUser("test-user");
        context.setProfileScheme("localfile");
        context.setDataSource(path + "*.csv");

        Fragmenter fragmenter = new HdfsDataFragmenter();
        fragmenter.initialize(context);

        List<Fragment> fragmentList = fragmenter.getFragments();
        assertNotNull(fragmentList);
        // empty.csv gets ignored
        assertEquals(3, fragmentList.size());
    }

    @Test
    public void testInvalidInputPath() throws Exception {
        thrown.expect(InvalidInputException.class);
        thrown.expectMessage("Input Pattern file:/tmp/non-existent-path-on-disk/*.csv matches 0 files");

        RequestContext context = new RequestContext();
        context.setConfig("default");
        context.setUser("test-user");
        context.setProfileScheme("localfile");
        context.setDataSource("/tmp/non-existent-path-on-disk/*.csv");

        Fragmenter fragmenter = new HdfsDataFragmenter();
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

        Fragmenter fragmenter = new HdfsDataFragmenter();
        fragmenter.initialize(context);

        List<Fragment> fragmentList = fragmenter.getFragments();
        assertNotNull(fragmentList);
        assertEquals(0, fragmentList.size());
    }
}