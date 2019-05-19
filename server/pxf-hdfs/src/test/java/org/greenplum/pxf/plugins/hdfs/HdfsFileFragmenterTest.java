package org.greenplum.pxf.plugins.hdfs;

import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.Fragmenter;
import org.greenplum.pxf.api.model.RequestContext;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Francisco Guerrero (me@frankgh.com)
 * @since 1.0 (5/19/19 7:41 AM)
 */
public class HdfsFileFragmenterTest {

    @Test
    public void testFragmeneterReturnsListOfFiles() throws Exception {
        String path = this.getClass().getClassLoader().getResource("csv/").getPath();

        RequestContext context = new RequestContext();
        context.setProtocol("localfile");
        context.setDataSource(path);

        Fragmenter fragmenter = new HdfsFileFragmenter();
        fragmenter.initialize(context);

        List<Fragment> fragmentList = fragmenter.getFragments();
        assertNotNull(fragmentList);
        assertEquals(4, fragmentList.size());
    }

}