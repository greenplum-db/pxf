package org.greenplum.pxf.automation.testplugin;

import org.greenplum.pxf.api.model.BaseFragmenter;
import org.greenplum.pxf.api.model.Fragment;

import java.util.List;

public class FaultyGUCFragmenter extends BaseFragmenter {

    @Override
    public List<Fragment> getFragments() throws Exception {
		throw new Exception(getClass().getSimpleName() + ": login " +
							context.getLogin() + " secret " +
							context.getSecret());
    }
}
