package org.greenplum.pxf.service.bridge;

import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.WriteVectorizedResolver;

import java.util.List;

public class TestWriteVectorizedResolver extends TestResolver implements WriteVectorizedResolver {
    @Override
    public int getBatchSize() {
        return 0;
    }

    @Override
    public OneRow setFieldsForBatch(List<List<OneField>> records) {
        return null;
    }
}
