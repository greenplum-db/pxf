package org.greenplum.pxf.service.bridge;

import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.ReadVectorizedResolver;

import java.util.List;

public class TestReadVectorizedResolver extends TestResolver implements ReadVectorizedResolver {
    @Override
    public List<List<OneField>> getFieldsForBatch(OneRow batch) {
        return null;
    }
}
