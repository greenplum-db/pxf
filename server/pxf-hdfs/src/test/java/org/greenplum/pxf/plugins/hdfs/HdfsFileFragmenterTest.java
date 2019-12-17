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

/**
 * Test the HdfsFileFragmenter
 */
public class HdfsFileFragmenterTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testFragmenterErrorsWhenPathDoesNotExist() throws Exception {
        expectedException.expect(InvalidInputException.class);
        expectedException.expectMessage("Input path does not exist:");

        String path = this.getClass().getClassLoader().getResource("csv/").getPath();

        RequestContext context = new RequestContext();
        context.setConfig("default");
        context.setUser("test-user");
        context.setProfileScheme("localfile");
        context.setDataSource(path + "non-existent");

        Fragmenter fragmenter = new HdfsFileFragmenter();
        fragmenter.initialize(context);
        fragmenter.getFragments();
    }

    @Test
    public void testFragmenterReturnsListOfFiles() throws Exception {
        String path = this.getClass().getClassLoader().getResource("csv/").getPath();

        RequestContext context = new RequestContext();
        context.setConfig("default");
        context.setUser("test-user");
        context.setProfileScheme("localfile");
        context.setDataSource(path);

        Fragmenter fragmenter = new HdfsFileFragmenter();
        fragmenter.initialize(context);

        List<Fragment> fragmentList = fragmenter.getFragments();
        assertNotNull(fragmentList);
        assertEquals(4, fragmentList.size());
    }

    @Test
    public void testFragmenterWilcardPath() throws Exception {
        String path = this.getClass().getClassLoader().getResource("csv/").getPath();

        RequestContext context = new RequestContext();
        context.setConfig("default");
        context.setUser("test-user");
        context.setProfileScheme("localfile");
        context.setDataSource(path + "*.csv");

        Fragmenter fragmenter = new HdfsFileFragmenter();
        fragmenter.initialize(context);

        List<Fragment> fragmentList = fragmenter.getFragments();
        assertNotNull(fragmentList);
        assertEquals(4, fragmentList.size());
    }
}
