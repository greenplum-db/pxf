package org.greenplum.pxf.api.model;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class BaseFragmenterTest {

    @Test
    public void testGetFragmentStatsIsUnsupported() throws Exception {
        Exception e = assertThrows(UnsupportedOperationException.class,
                () -> new BaseFragmenter().getFragmentStats());
        assertEquals("Operation getFragmentStats is not supported", e.getMessage());
    }

    @Test
    public void testGetFragments() throws Exception {
        List<Fragment> fragments = new BaseFragmenter().getFragments();
        assertNotNull(fragments);
        assertEquals(0, fragments.size());
    }

    @Test
    public void testBaseFragmenterIsNotInitialized() {
        assertFalse(new BaseFragmenter().isInitialized());
    }

}
