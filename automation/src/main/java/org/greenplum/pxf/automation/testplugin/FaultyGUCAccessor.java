package org.greenplum.pxf.automation.testplugin;

import org.greenplum.pxf.api.ReadAccessor;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.utilities.InputData;
import org.greenplum.pxf.api.utilities.Plugin;

public class FaultyGUCAccessor extends Plugin implements ReadAccessor {
    public FaultyGUCAccessor(InputData metaData) {
        super(metaData);
    }

    @Override
    public boolean openForRead() throws Exception {
		throw new Exception(getClass().getSimpleName() + ": login " + 
							inputData.getLogin() + " secret " + 
							inputData.getSecret());
    }

    @Override
    public OneRow readNextObject() throws Exception {
		throw new Exception("not implemented");
    }

    @Override
    public void closeForRead() throws Exception {
		throw new Exception("not implemented");
    }
}
