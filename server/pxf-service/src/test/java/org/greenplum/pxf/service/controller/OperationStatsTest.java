package org.greenplum.pxf.service.controller;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class OperationStatsTest {
    @Test
    public void testDefaultValues() {
        OperationStats defaultStats = OperationStats.builder().build();

        assertEquals(null, defaultStats.getOperation());
        assertEquals(0L, defaultStats.getRecordCount());
        assertEquals(0L, defaultStats.getBatchCount());
        assertEquals(0L, defaultStats.getByteCount());
    }

    @Test
    public void testUpdate() {
        OperationStats startingStats = OperationStats.builder().operation("read").build();
        OperationStats addMe = OperationStats.builder().operation("addMe").recordCount(10L).batchCount(15L).byteCount(20L).build();

        startingStats.update(addMe);

        assertEquals("read", startingStats.getOperation());
        assertEquals(10L, startingStats.getRecordCount());
        assertEquals(15L, startingStats.getBatchCount());
        assertEquals(20L, startingStats.getByteCount());
    }
}
