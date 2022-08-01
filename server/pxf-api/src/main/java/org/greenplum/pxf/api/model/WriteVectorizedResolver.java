package org.greenplum.pxf.api.model;

import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;

import java.util.List;

/**
 *
 * Interface that creates a single batch from a list of records.
 *
 */
public interface WriteVectorizedResolver {

    /**
     * Returns the size of the batch that resolver recommends
     * @return size of the batch
     */
    int getBatchSize();

    /**
     * Constructs and sets the fields of a batch of records represented by {@link OneRow}.
     *
     * @param records list of records, each record is a list of {@link OneField} objects
     * @return the constructed {@link OneRow} which is a batch
     * @throws Exception if constructing of a row batch from the fields failed
     */
    OneRow setFieldsForBatch(List<List<OneField>> records) throws Exception;

}
