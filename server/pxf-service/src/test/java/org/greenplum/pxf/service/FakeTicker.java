package org.greenplum.pxf.service;

import com.google.common.base.Ticker;

import java.util.concurrent.atomic.AtomicLong;

public class FakeTicker extends Ticker {
    private final AtomicLong nanos = new AtomicLong();

    @Override
    public long read() {
        return nanos.get();
    }

    public void advanceTime(long milliseconds) {
        nanos.addAndGet(milliseconds * UGICache.NANOS_PER_MILLIS);
    }
}
