package org.greenplum.pxf.automation.structures.tables.pxf;

/**
 * represents GPDB -> PXF Readable External Table
 */
public class ReadableExternalTable extends ExternalTable {

	public ReadableExternalTable(String name, String[] fields, String path, String format) {
		super(name, fields, path, format);
	}
}
