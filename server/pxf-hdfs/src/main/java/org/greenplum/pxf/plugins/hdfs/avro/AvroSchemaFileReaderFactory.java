package org.greenplum.pxf.plugins.hdfs.avro;

import org.springframework.stereotype.Component;

@Component
public class AvroSchemaFileReaderFactory {

    /**
     * Returns the Avro schema file reader given the name of the schema file
     *
     * @param schemaFile the name of the schema file
     * @return the Avro schema file reader given the name of the schema file
     */
    public AvroSchemaFileReader getAvroSchemaFileReader(String schemaFile) {
        if (schemaFile.matches("^.*\\.avsc$")) {
            return new JsonAvroSchemaFileReader();
        }
        return new BinaryAvroSchemaFileReader();
    }
}
