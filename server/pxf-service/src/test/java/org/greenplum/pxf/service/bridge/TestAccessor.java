package org.greenplum.pxf.service.bridge;

import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.Accessor;
import org.springframework.stereotype.Component;

@Component
public class TestAccessor implements Accessor {

    @Override
    public boolean openForRead() {
        return false;
    }

    @Override
    public OneRow readNextObject() {
        return null;
    }

    @Override
    public void closeForRead() {

    }

    @Override
    public boolean openForWrite() {
        return false;
    }

    @Override
    public boolean writeNextObject(OneRow onerow) {
        return false;
    }

    @Override
    public void closeForWrite() {

    }

    @Override
    public boolean isThreadSafe() {
        return false;
    }

}
