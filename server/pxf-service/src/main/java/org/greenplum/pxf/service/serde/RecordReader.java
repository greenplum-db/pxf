package org.greenplum.pxf.service.serde;

import org.greenplum.pxf.api.OneField;

import java.io.DataInput;
import java.util.List;

/**
 * Interface for deserialization of an input stream with data from Greenplum into a List of OneField objects
 * for downstream consumption by resolvers. The implementations of this interface deal with the actual
 * specifics of how data is serialized by Greenplum PXF extension for different formatting specifications.
 */
public interface RecordReader {

    /**
     * Reads the provided input stream received from GPDB and deserializes a database tuple according to the
     * outputFormat specification. The tuple is deserialized into a List of OneField objects that are used by
     * a downstream resolver to construct data representation appropriate for the external system.
     * @param input a data input stream
     * @return a list of OneField objects, generally corresponding to columns of a database tuple
     * @throws Exception if the operation fails
     */
    List<OneField> readRecord(DataInput input) throws Exception;
}
