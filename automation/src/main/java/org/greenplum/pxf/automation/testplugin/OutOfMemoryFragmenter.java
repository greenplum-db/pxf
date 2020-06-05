package org.greenplum.pxf.automation.testplugin;

import org.greenplum.pxf.api.model.BaseFragmenter;
import org.greenplum.pxf.api.model.Fragment;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;

import java.util.LinkedList;
import java.util.List;

@Component("OutOfMemoryFragmenter")
@RequestScope
public class OutOfMemoryFragmenter extends BaseFragmenter {

    @Override
    public List<Fragment> getFragments() throws Exception {
        List<Object> objectList = new LinkedList<>();
        while (true) {
            byte[] a = new byte[10 * 1024 * 1024];
            objectList.add(a);
        }
    }
}
