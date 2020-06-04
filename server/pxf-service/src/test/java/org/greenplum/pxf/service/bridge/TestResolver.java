package org.greenplum.pxf.service.bridge;

import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.Resolver;

import java.util.List;

public class TestResolver implements Resolver {

    @Override
    public void afterPropertiesSet() {
    }

    @Override
    public List<OneField> getFields(OneRow row) {
        return null;
    }

    @Override
    public OneRow setFields(List<OneField> record) {
        return null;
    }

    @Override
    public void setRequestContext(RequestContext context) {
    }

    @Override
    public boolean isThreadSafe() {
        return false;
    }
}
