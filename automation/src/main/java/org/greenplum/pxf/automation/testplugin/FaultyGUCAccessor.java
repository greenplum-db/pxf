package org.greenplum.pxf.automation.testplugin;

import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.BasePlugin;

public class FaultyGUCAccessor extends BasePlugin implements Accessor {

    @Override
    public boolean openForRead() throws Exception {
		throw new Exception(getClass().getSimpleName() + ": login " +
							context.getLogin() + " secret " +
							context.getSecret());
    }

    @Override
    public OneRow readNextObject() throws Exception {
		throw new UnsupportedOperationException("readNextObject method is not implemented");
    }

    @Override
    public void closeForRead() throws Exception {
        throw new UnsupportedOperationException("closeForRead method is not implemented");
    }

    @Override
    public boolean openForWrite() throws Exception {
        throw new UnsupportedOperationException("openForWrite method is not implemented");
    }

    @Override
    public boolean writeNextObject(OneRow onerow) throws Exception {
        throw new UnsupportedOperationException("writeNextObject method is not implemented");
    }

    @Override
    public void closeForWrite() throws Exception {
        throw new UnsupportedOperationException("closeForWrite method is not implemented");
    }
}
