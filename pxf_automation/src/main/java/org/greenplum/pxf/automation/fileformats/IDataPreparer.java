package org.greenplum.pxf.automation.fileformats;

import org.greenplum.pxf.automation.structures.tables.basic.Table;


/**
 * functionality for classes that preparing sort of data such Avro, Sequence, Text, Protobuf
 * 
 */
public interface IDataPreparer {

	/**
	 * Get amount of Data rows to create, storing the data in the dataTable data list.
	 * 
	 * @param rows amount of data to generate
	 * @param dataTable dataTable structure to save the generated data
	 * @return Array of Data
	 * @throws Exception
	 */
	public Object[] prepareData(int rows, Table dataTable) throws Exception;
}
