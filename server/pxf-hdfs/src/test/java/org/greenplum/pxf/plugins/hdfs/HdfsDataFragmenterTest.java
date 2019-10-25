package org.greenplum.pxf.plugins.hdfs;

import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.Fragmenter;
import org.greenplum.pxf.api.model.RequestContext;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class HdfsDataFragmenterTest {

    @Test
    public void testFragmenterReturnsListOfFiles() throws Exception {
        String path = this.getClass().getClassLoader().getResource("csv/").getPath();

        RequestContext context = new RequestContext();
        context.setConfig("default");
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
        context.setProfileScheme("localfile");
        context.setDataSource(path + "*.csv");

        Fragmenter fragmenter = new HdfsDataFragmenter();
        fragmenter.initialize(context);

        List<Fragment> fragmentList = fragmenter.getFragments();
        assertNotNull(fragmentList);
        // empty.csv gets ignored
        assertEquals(3, fragmentList.size());
    }
}