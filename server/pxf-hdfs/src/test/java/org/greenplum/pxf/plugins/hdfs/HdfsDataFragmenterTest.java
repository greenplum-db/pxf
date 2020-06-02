package org.greenplum.pxf.plugins.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InvalidInputException;
import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.Fragmenter;
import org.greenplum.pxf.api.model.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class HdfsDataFragmenterTest {

    private final RequestContext context = new RequestContext();

    @BeforeEach
    public void setup() {
        context.setConfiguration(new Configuration());
    }

    @Test
    public void testFragmenterReturnsListOfFiles() throws Exception {
        String path = this.getClass().getClassLoader().getResource("csv/").getPath();

        context.setConfig("default");
        context.setUser("test-user");
        context.setProfileScheme("localfile");
        context.setDataSource(path);

        Fragmenter fragmenter = getFragmenter(context);

        List<Fragment> fragmentList = fragmenter.getFragments();
        assertNotNull(fragmentList);
        // empty.csv gets ignored
        assertEquals(3, fragmentList.size());
    }

    @Test
    public void testFragmenterWilcardPath() throws Exception {
        String path = this.getClass().getClassLoader().getResource("csv/").getPath();

        context.setConfig("default");
        context.setUser("test-user");
        context.setProfileScheme("localfile");
        context.setDataSource(path + "*.csv");

        Fragmenter fragmenter = getFragmenter(context);

        List<Fragment> fragmentList = fragmenter.getFragments();
        assertNotNull(fragmentList);
        // empty.csv gets ignored
        assertEquals(3, fragmentList.size());
    }

    @Test
    public void testInvalidInputPath() {
        context.setConfig("default");
        context.setUser("test-user");
        context.setProfileScheme("localfile");
        context.setDataSource("/tmp/non-existent-path-on-disk/*.csv");

        Fragmenter fragmenter = getFragmenter(context);
        Exception e = assertThrows(InvalidInputException.class,
                fragmenter::getFragments);
        assertEquals("Input Pattern file:/tmp/non-existent-path-on-disk/*.csv matches 0 files", e.getMessage());
    }

    @Test
    public void testInvalidInputPathIgnored() throws Exception {
        context.setConfig("default");
        context.setUser("test-user");
        context.setProfileScheme("localfile");
        context.addOption("IGNORE_MISSING_PATH", "true");
        context.setDataSource("/tmp/non-existent-path-on-disk/*.csv");

        Fragmenter fragmenter = getFragmenter(context);

        List<Fragment> fragmentList = fragmenter.getFragments();
        assertNotNull(fragmentList);
        assertEquals(0, fragmentList.size());
    }

    private Fragmenter getFragmenter(RequestContext context) {
        Fragmenter fragmenter = new HdfsDataFragmenter();
        fragmenter.setRequestContext(context);
        fragmenter.initialize();
        return fragmenter;
    }
}
