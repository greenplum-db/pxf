package org.greenplum.pxf.plugins.hbase;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.filter.FilterParser;
import org.greenplum.pxf.api.filter.Node;
import org.greenplum.pxf.api.filter.Operator;
import org.greenplum.pxf.api.filter.SupportedOperatorPruner;
import org.greenplum.pxf.api.filter.TreeTraverser;
import org.greenplum.pxf.api.filter.TreeVisitor;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.BasePlugin;
import org.greenplum.pxf.plugins.hbase.utilities.HBaseColumnDescriptor;
import org.greenplum.pxf.plugins.hbase.utilities.HBaseTupleDescription;
import org.greenplum.pxf.plugins.hbase.utilities.HBaseUtilities;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.EnumSet;

/**
 * Accessor for HBase.
 * This class is responsible for opening the HBase table requested and
 * for iterating over its relevant fragments (regions) to return the relevant table's rows.
 * <p>
 * The table is divided into several splits. Each accessor instance is assigned a single split.
 * For each region, a Scan object is used to describe the requested rows.
 * <p>
 * The class supports filters using the {@link HBaseFilterBuilder}.
 * Regions can be filtered out according to input from {@link HBaseFilterBuilder}.
 */
public class HBaseAccessor extends BasePlugin implements Accessor {

    static final EnumSet<Operator> SUPPORTED_OPERATORS =
            EnumSet.of(
                    Operator.LESS_THAN,
                    Operator.GREATER_THAN,
                    Operator.LESS_THAN_OR_EQUAL,
                    Operator.GREATER_THAN_OR_EQUAL,
                    Operator.EQUALS,
                    Operator.NOT_EQUALS,
                    Operator.IS_NOT_NULL,
                    Operator.IS_NULL,
                    Operator.AND,
                    Operator.OR
            );

    private static TreeVisitor TREE_PRUNER = new SupportedOperatorPruner(SUPPORTED_OPERATORS);
    private static TreeTraverser TREE_TRAVERSER = new TreeTraverser();

    private HBaseTupleDescription tupleDescription;
    private Connection connection;
    private Table table;
    private SplitBoundary split;
    private Scan scanDetails;
    private ResultScanner currentScanner;
    private byte[] scanStartKey;
    private byte[] scanEndKey;

    /**
     * The class represents a single split of a table
     * i.e. a start key and an end key
     */
    private static class SplitBoundary {
        protected byte[] startKey;
        protected byte[] endKey;

        SplitBoundary(byte[] first, byte[] second) {
            startKey = first;
            endKey = second;
        }

        byte[] startKey() {
            return startKey;
        }

        byte[] endKey() {
            return endKey;
        }
    }

    /**
     * Initializes HBaseAccessor based on GPDB table description and
     * initializes the scan start and end keys of the HBase table to default values.
     *
     * @param requestContext data provided in the request
     */
    @Override
    public void initialize(RequestContext requestContext) {
        super.initialize(requestContext);

        tupleDescription = new HBaseTupleDescription(context);
        split = null;
        scanStartKey = HConstants.EMPTY_START_ROW;
        scanEndKey = HConstants.EMPTY_END_ROW;
    }

    /**
     * Opens the HBase table.
     *
     * @return true if the current fragment (split) is
     * available for reading and includes in the filter
     */
    @Override
    public boolean openForRead() throws Exception {
        openTable();
        createScanner();
        addTableSplit();

        return openCurrentRegion();
    }

    /**
     * Closes the HBase table.
     */
    @Override
    public void closeForRead() throws Exception {
        table.close();
        HBaseUtilities.closeConnection(null, connection);
    }

    /**
     * Opens the resource for write.
     *
     * @return true if the resource is successfully opened
     */
    @Override
    public boolean openForWrite() {
        throw new UnsupportedOperationException();
    }

    /**
     * Writes the next object.
     *
     * @param onerow the object to be written
     * @return true if the write succeeded
     */
    @Override
    public boolean writeNextObject(OneRow onerow) {
        throw new UnsupportedOperationException();
    }

    /**
     * Closes the resource for write.
     */
    @Override
    public void closeForWrite() {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns the next row in the HBase table, null if end of fragment.
     */
    @Override
    public OneRow readNextObject() throws IOException {
        Result result;

        // while currentScanner can't return a new result
        if ((result = currentScanner.next()) == null) {
            currentScanner.close(); // close it
            return null; // no more rows on the split
        }

        return new OneRow(null, result);
    }

    /**
     * Load hbase table object using ConnectionFactory
     */
    private void openTable() throws IOException {
        connection = ConnectionFactory.createConnection(HBaseConfiguration.create(configuration));
        table = connection.getTable(TableName.valueOf(context.getDataSource()));
    }

    /**
     * Creates a {@link SplitBoundary} of the table split
     * this accessor instance is assigned to scan.
     * The table split is constructed from the fragment metadata
     * passed in {@link RequestContext#getFragmentMetadata()}.
     * <p>
     * The function verifies the split is within user supplied range.
     * <p>
     * It is assumed, |startKeys| == |endKeys|
     * This assumption is made through HBase's code as well.
     */
    private void addTableSplit() {

        byte[] serializedMetadata = context.getFragmentMetadata();
        if (serializedMetadata == null) {
            throw new IllegalArgumentException("Missing fragment metadata information");
        }
        try {
            ByteArrayInputStream bytesStream = new ByteArrayInputStream(serializedMetadata);
            ObjectInputStream objectStream = new ObjectInputStream(bytesStream);

            byte[] startKey = (byte[]) objectStream.readObject();
            byte[] endKey = (byte[]) objectStream.readObject();

            if (withinScanRange(startKey, endKey)) {
                split = new SplitBoundary(startKey, endKey);
            }
        } catch (Exception e) {
            throw new RuntimeException("Exception while reading expected fragment metadata", e);
        }
    }

    /**
     * Returns true if given start/end key pair is within the scan range.
     */
    private boolean withinScanRange(byte[] startKey, byte[] endKey) {

        // startKey <= scanStartKey
        if (Bytes.compareTo(startKey, scanStartKey) <= 0) {
            // endKey == table's end or endKey >= scanStartKey
            return Bytes.equals(endKey, HConstants.EMPTY_END_ROW) ||
                    Bytes.compareTo(endKey, scanStartKey) >= 0;
        } else { // startKey > scanStartKey
            // scanEndKey == table's end or startKey <= scanEndKey
            return Bytes.equals(scanEndKey, HConstants.EMPTY_END_ROW) ||
                    Bytes.compareTo(startKey, scanEndKey) <= 0;
        }
    }

    /**
     * Creates the Scan object used to describe the query
     * requested from HBase.
     * As the row key column always gets returned, no need to ask for it.
     */
    private void createScanner() throws Exception {
        scanDetails = new Scan();
        // Return only one version (latest)
        scanDetails.setMaxVersions(1);

        addColumns();
        addFilters();
    }

    /**
     * Opens the region of the fragment to be scanned.
     * Updates the Scan object to retrieve only rows from that region.
     */
    private boolean openCurrentRegion() throws IOException {
        if (split == null) {
            return false;
        }

        scanDetails.setStartRow(split.startKey());
        scanDetails.setStopRow(split.endKey());

        currentScanner = table.getScanner(scanDetails);
        return true;
    }

    /**
     * Adds the table tuple description to {@link #scanDetails},
     * so only these fields will be returned.
     */
    private void addColumns() {
        for (int i = 0; i < tupleDescription.columns(); ++i) {
            HBaseColumnDescriptor column = tupleDescription.getColumn(i);
            if (!column.isKeyColumn()) // Row keys return anyway
            {
                scanDetails.addColumn(column.columnFamilyBytes(), column.qualifierBytes());
            }
        }
    }

    /**
     * Uses {@link HBaseFilterBuilder} to translate a filter string into a
     * HBase {@link Filter} object. The result is added as a filter to the
     * Scan object.
     * <p>
     * Uses row key ranges to limit split count.
     */
    private void addFilters() throws Exception {
        if (!context.hasFilter()) {
            return;
        }

        HBaseFilterBuilder hBaseFilterBuilder = new HBaseFilterBuilder(tupleDescription);
        Node root = new FilterParser().parse(context.getFilterString().getBytes(FilterParser.DEFAULT_CHARSET));
        root = TREE_PRUNER.visit(root);

        TREE_TRAVERSER.inOrderTraversal(root, hBaseFilterBuilder);

        Filter filter = hBaseFilterBuilder.buildFilter();
        scanDetails.setFilter(filter);

        scanStartKey = hBaseFilterBuilder.getStartKey();
        scanEndKey = hBaseFilterBuilder.getEndKey();
    }
}
