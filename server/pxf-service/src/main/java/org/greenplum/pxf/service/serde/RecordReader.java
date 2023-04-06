package org.greenplum.pxf.service.serde;

import org.greenplum.pxf.api.OneField;

import java.io.DataInput;
import java.util.List;

public interface RecordReader {

    /**
     * Reads the provided input stream received from GPDB and deserializes a database tuple according to the
     * outputFormat specification. The tuple is deserialized into a List of OneField objects that will be used by
     * a downstream resolver to construct data representation appropriate for the external system.
     * @param input a data input stream
     * @return a list of OneField objects, generally corresponding to columns of a database tuple
     * @throws Exception if the operation fails
     */
    List<OneField> readRecord(DataInput input) throws Exception;
}
