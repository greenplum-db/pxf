package org.greenplum.pxf.api.model;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class BaseFragmenterTest {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testGetFragmentStatsIsUnsupported() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Operation getFragmentStats is not supported");

        new BaseFragmenter().getFragmentStats();
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
