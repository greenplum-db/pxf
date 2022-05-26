package org.greenplum.pxf.api.model;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class BaseFragmenterTest {

    RequestContext context;
    BaseFragmenter baseFragmenter;

    @BeforeEach
    public void setup() {
        context = new RequestContext();
        context.setProfile("noprofile");

        baseFragmenter = new BaseFragmenter();
        baseFragmenter.setRequestContext(context);
    }

    @Test
    public void testGetFragmentStatsIsUnsupported() {

        Exception e = assertThrows(UnsupportedOperationException.class,
                () -> baseFragmenter.getFragmentStats());
        assertEquals("Profile 'noprofile' does not support statistics for fragments", e.getMessage());
    }

    @Test
    public void testGetFragments() throws Exception {
        List<Fragment> fragments = new BaseFragmenter().getFragments();
        assertNotNull(fragments);
        assertEquals(0, fragments.size());
    }
}
