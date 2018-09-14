package org.greenplum.pxf.automation.fileformats;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.generic.GenericRecord;

/**
 * Each Avro Schema class need to implement this interface. It Contains serialization methods for
 * Stand alone avro and for Avro written inside sequence file.
 */
public interface IAvroSchema {

	/**
	 * 
	 * @return Serialized Generic Record of one record data
	 * @throws IOException
	 */
	public GenericRecord serialize() throws IOException;

	/**
	 * For Avro inside sequence file, get the serialized sequence value as stream.
	 * 
	 * @param stream
	 * @throws IOException
	 */
	public void serialize(ByteArrayOutputStream stream) throws IOException;
}