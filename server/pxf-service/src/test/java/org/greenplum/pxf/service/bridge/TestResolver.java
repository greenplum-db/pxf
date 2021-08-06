package org.greenplum.pxf.service.bridge;

import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.Resolver;

import java.util.List;

public class TestResolver implements Resolver {

    @Override
    public List<OneField> getFields(OneRow row) {
        return null;
    }

    @Override
    public OneRow setFields(List<OneField> record) {
        return null;
    }

    @Override
    public void initialize(RequestContext requestContext) {
    }

    @Override
    public Configuration getConfiguration() {
        return null;
    }
}
