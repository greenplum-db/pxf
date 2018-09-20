package org.greenplum.pxf.automation.structures.tables.hbase;

import java.util.List;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;

import org.greenplum.pxf.automation.structures.tables.basic.Table;

/**
 * represents HBase Table
 */
public class HBaseTable extends Table {

	private String[] qualifiers;
	private int numberOfSplits;
	private String rowKeyPrefix = "row";
	private int rowsPerSplit;
	private FilterList filters;
	private int rowLimit = -1;

    protected List<Put> rowsToGenerate;

	@Override
	public void initDataStructures() {

		super.initDataStructures();

		filters = null;

		addColumnHeader("rowId");
	}

	public HBaseTable(String name, String[] colsFamily) {
		super(name, colsFamily);
	}

	public int getNumberOfSplits() {
		return numberOfSplits;
	}

	public void setNumberOfSplits(int numberOfSplits) {
		this.numberOfSplits = numberOfSplits;
	}

	public String getRowKeyPrefix() {
		return rowKeyPrefix;
	}

	public void setRowKeyPrefix(String rowKeyPrefix) {
		this.rowKeyPrefix = rowKeyPrefix;
	}

	public int getRowsPerSplit() {
		return rowsPerSplit;
	}

	public void setRowsPerSplit(int rowsPerSplit) {
		this.rowsPerSplit = rowsPerSplit;
	}

	public String[] getQualifiers() {
		return qualifiers;
	}

	public void setQualifiers(String[] qualifiers) {
		this.qualifiers = qualifiers;
	}

	public void addFilter(Filter filter) {
		if (filters == null) {
			filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		}

		filters.addFilter(filter);
	}

	public FilterList getFilters() {
		return filters;
	}

	public void setFilters(FilterList filters) {
		this.filters = filters;
	}

	public int getRowLimit() {
		return rowLimit;
	}

	public void setRowLimit(int rowLimit) {
		this.rowLimit = rowLimit;
	}

	public List<Put> getRowsToGenerate() {
		return rowsToGenerate;
	}

	public void setRowsToGenerate(List<Put> rowsToGenerate) {
		this.rowsToGenerate = rowsToGenerate;
	}
}
