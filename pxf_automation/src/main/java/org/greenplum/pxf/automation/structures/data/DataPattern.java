package org.greenplum.pxf.automation.structures.data;

import java.util.ArrayList;

/*
 * Class for meta data, hold properties of the data to be created and queried later by gpdb.
 * Used to create tables from configuration (like json) and to be tested by automation. 
 */
public class DataPattern {
	
	private String columnDelimiter = ",";  
	private ArrayList<String>  coulmnsTypeList; 

	private int columnMaxSize;
	private long dataSizeInMegaBytes;
	private boolean randomValues = false;
	private String recordValues = null; 
	
	// Getters
	public ArrayList<String> getColumnsTypeList() {
		return coulmnsTypeList;
	}
	
	public int getColumnMaxSize() {
		return columnMaxSize;
	}
	
	public String getColumnDelimiter() {
		return columnDelimiter;
	}
	
	public long getDataSizeInMegaBytes() {
		return dataSizeInMegaBytes;
	}
	
	public boolean isRandomValues() {
		return randomValues;
	}
	
	public String getRecordValues() {
		return recordValues;
	}
	
	// Setters
	public void setCoulmnsTypeList(ArrayList<String> coulmnsTypeList) {
		this.coulmnsTypeList = coulmnsTypeList;
	}

	public void setColumnMaxSize(int columnMaxSize) {
		this.columnMaxSize = columnMaxSize;
	}

	public void setColumnDelimiter(String columnDelimiter) {
		this.columnDelimiter = columnDelimiter;
	}

	public void setDataSizeInMegaBytes(long dataSizeInMegaBytes) {
		this.dataSizeInMegaBytes = dataSizeInMegaBytes;
	}
	
	public void setRandomValues(boolean randomValues) {
		this.randomValues = randomValues; 
	}
	
	public void setRecordValues(String recordValues) {
		this.recordValues = recordValues;
	}
	
	@Override
	public String toString() {
		return "DataPattern [columnDelimiter=" + columnDelimiter + ", coulmnsTypeList=" + coulmnsTypeList + ", columnMaxSize=" + columnMaxSize + ", dataSizeInMegaBytes=" + dataSizeInMegaBytes + ", randomValues=" + randomValues + ", recordValues=" + recordValues + "]";
	}
}
