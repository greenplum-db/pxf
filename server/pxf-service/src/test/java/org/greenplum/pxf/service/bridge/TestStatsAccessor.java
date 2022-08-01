package org.greenplum.pxf.service.bridge;

import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.StatsAccessor;

public class TestStatsAccessor extends TestAccessor implements StatsAccessor {
    @Override
    public void retrieveStats() {
    }

    @Override
    public OneRow emitAggObject() {
        return null;
    }
}
