package org.greenplum.pxf.service.bridge;

import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.RequestContext;

public class TestAccessor implements Accessor {

    @Override
    public boolean openForRead() throws Exception {
        return false;
    }

    @Override
    public OneRow readNextObject() throws Exception {
        return null;
    }

    @Override
    public void closeForRead() throws Exception {

    }

    @Override
    public boolean openForWrite() throws Exception {
        return false;
    }

    @Override
    public boolean writeNextObject(OneRow onerow) throws Exception {
        return false;
    }

    @Override
    public void closeForWrite() throws Exception {

    }

    @Override
    public void initialize(RequestContext requestContext) {

    }

    @Override
    public boolean isThreadSafe() {
        return false;
    }

}
