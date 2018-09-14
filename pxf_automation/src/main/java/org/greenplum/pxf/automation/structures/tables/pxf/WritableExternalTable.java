package org.greenplum.pxf.automation.structures.tables.pxf;

/**
 * represents HAWQ -> PXF Writable External Table
 */
public class WritableExternalTable extends ReadableExternalTable {

	public WritableExternalTable(String name, String[] fields, String path, String format) {
		super(name, fields, path, format);
	}

	private String compressionCodec = null;

	@Override
	protected String createHeader() {
		return "CREATE WRITABLE EXTERNAL TABLE " + getFullName();
	}

	@Override
	protected String getLocationParameters() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.getLocationParameters());

		if (compressionCodec != null) {
			appendParamter(sb, "COMPRESSION_CODEC=" + getCompressionCodec());
		}

		return sb.toString();
	}

	public String getCompressionCodec() {
		return compressionCodec;
	}

	public void setCompressionCodec(String compressionCodec) {
		this.compressionCodec = compressionCodec;
	}
}
