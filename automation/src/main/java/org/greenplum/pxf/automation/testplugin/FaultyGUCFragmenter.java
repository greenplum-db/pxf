package org.greenplum.pxf.automation.testplugin;

import org.greenplum.pxf.api.model.BaseFragmenter;
import org.greenplum.pxf.api.model.Fragment;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;

import java.util.List;

@Component("FaultyGUCFragmenter")
@RequestScope
public class FaultyGUCFragmenter extends BaseFragmenter {

    @Override
    public List<Fragment> getFragments() throws Exception {
		throw new Exception(getClass().getSimpleName() + ": login " +
							context.getLogin() + " secret " +
							context.getSecret());
    }
}
