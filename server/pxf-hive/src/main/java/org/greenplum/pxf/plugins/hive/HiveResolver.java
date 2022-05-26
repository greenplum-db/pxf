package org.greenplum.pxf.plugins.hive;

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

import org.apache.commons.lang.CharUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.error.BadRecordException;
import org.greenplum.pxf.api.error.UnsupportedTypeException;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.BasePlugin;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.Resolver;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.api.utilities.SpringContext;
import org.greenplum.pxf.api.utilities.Utilities;
import org.greenplum.pxf.plugins.hdfs.utilities.HdfsUtilities;
import org.greenplum.pxf.plugins.hive.utilities.HiveUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;

/**
 * Class HiveResolver handles deserialization of records that were serialized
 * using Hadoop's Hive serialization framework.
 */
public class HiveResolver extends BasePlugin implements Resolver {
    private static final Logger LOG = LoggerFactory.getLogger(HiveResolver.class);

    protected static final String MAPKEY_DELIM = ":";
    protected static final String COLLECTION_DELIM = ",";
    protected static final String nullChar = "\\N";

    protected char delimiter;
    protected String collectionDelim;
    protected String mapkeyDelim;
    protected Deserializer deserializer;
    protected String serdeClassName;
    protected List<Integer> hiveIndexes;
    protected HiveMetadata metadata;
    protected HiveUtilities hiveUtilities;

    private int numberOfPartitions;
    private Map<String, OneField> partitionColumnNames;
    private String hiveDefaultPartName;

    public HiveResolver() {
        this(SpringContext.getBean(HiveUtilities.class));
    }

    HiveResolver(HiveUtilities hiveUtilities) {
        this.hiveUtilities = hiveUtilities;
    }

    /**
     * Initializes the HiveResolver by parsing the request context and
     * obtaining the serde class name, the serde properties string and the
     * partition keys.
     */
    @Override
    public void afterPropertiesSet() {
        super.afterPropertiesSet();

        hiveDefaultPartName = HiveConf.getVar(configuration, HiveConf.ConfVars.DEFAULTPARTITIONNAME);

        try {
            parseUserData(context);
            initPartitionFields();
            initSerde();
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize HiveResolver", e);
        }
    }

    @Override
    public List<OneField> getFields(OneRow onerow) throws Exception {
        Object tuple = deserializer.deserialize((Writable) onerow.getData());
        // Each Hive record is a Struct
        StructObjectInspector soi = (StructObjectInspector) deserializer.getObjectInspector();
        return traverseStruct(tuple, soi, false);
    }

    /**
     * Constructs and sets the fields of a {@link OneRow}.
     *
     * @param record list of {@link OneField}
     * @return the constructed {@link OneRow}
     */
    @Override
    public OneRow setFields(List<OneField> record) {
        throw new UnsupportedOperationException("Hive resolver does not support write operation.");
    }

    public int getNumberOfPartitions() {
        return numberOfPartitions;
    }

    /* Parses user data string (received from fragmenter). */
    void parseUserData(RequestContext context) {
        // HiveMetadata is passed from accessor
        metadata = (HiveMetadata) context.getMetadata();
        if (metadata == null) {
            throw new RuntimeException("No hive metadata detected in request context");
        }
        collectionDelim = StringUtils.defaultString(context.getOption("COLLECTION_DELIM"), COLLECTION_DELIM);
        mapkeyDelim = StringUtils.defaultString(context.getOption("MAPKEY_DELIM"), MAPKEY_DELIM);
        hiveIndexes = metadata.getHiveIndexes();
        serdeClassName = metadata.getProperties().getProperty(SERIALIZATION_LIB);
    }

    /*
     * Gets and init the deserializer for the records of this Hive data
     * fragment.
     */
    void initSerde() throws Exception {
        Class<?> c = Class.forName(serdeClassName, true, JavaUtils.getClassLoader());
        deserializer = (Deserializer) c.getDeclaredConstructor().newInstance();
        deserializer.initialize(getJobConf(), getSerdeProperties());
    }

    protected JobConf getJobConf() {
        return new JobConf(configuration, this.getClass());
    }

    /*
     * The partition fields are initialized one time base on userData provided
     * by the fragmenter.
     */
    void initPartitionFields() {
        partitionColumnNames = new HashMap<>();

        List<HivePartition> hivePartitionList = metadata.getPartitions();
        if (hivePartitionList == null || hivePartitionList.size() == 0) {
            // no partition column information
            return;
        }

        for (HivePartition partition : hivePartitionList) {
            String columnName = partition.getName();
            String type = partition.getType();
            String val = partition.getValue();
            DataType convertedType;
            Object convertedValue;
            boolean isDefaultPartition;

            // check if value is default partition
            isDefaultPartition = isDefaultPartition(type, val);
            // ignore the type's parameters
            String typeName = type.replaceAll("\\(.*\\)", "");

            switch (typeName) {
                case serdeConstants.STRING_TYPE_NAME:
                    convertedType = DataType.TEXT;
                    convertedValue = isDefaultPartition ? null : val;
                    break;
                case serdeConstants.BOOLEAN_TYPE_NAME:
                    convertedType = DataType.BOOLEAN;
                    convertedValue = isDefaultPartition ? null
                            : Boolean.valueOf(val);
                    break;
                case serdeConstants.TINYINT_TYPE_NAME:
                case serdeConstants.SMALLINT_TYPE_NAME:
                    convertedType = DataType.SMALLINT;
                    convertedValue = isDefaultPartition ? null
                            : Short.parseShort(val);
                    break;
                case serdeConstants.INT_TYPE_NAME:
                    convertedType = DataType.INTEGER;
                    convertedValue = isDefaultPartition ? null
                            : Integer.parseInt(val);
                    break;
                case serdeConstants.BIGINT_TYPE_NAME:
                    convertedType = DataType.BIGINT;
                    convertedValue = isDefaultPartition ? null
                            : Long.parseLong(val);
                    break;
                case serdeConstants.FLOAT_TYPE_NAME:
                    convertedType = DataType.REAL;
                    convertedValue = isDefaultPartition ? null
                            : Float.parseFloat(val);
                    break;
                case serdeConstants.DOUBLE_TYPE_NAME:
                    convertedType = DataType.FLOAT8;
                    convertedValue = isDefaultPartition ? null
                            : Double.parseDouble(val);
                    break;
                case serdeConstants.TIMESTAMP_TYPE_NAME:
                    convertedType = DataType.TIMESTAMP;
                    convertedValue = isDefaultPartition ? null
                            : Timestamp.valueOf(val);
                    break;
                case serdeConstants.DATE_TYPE_NAME:
                    convertedType = DataType.DATE;
                    convertedValue = isDefaultPartition ? null
                            : Date.valueOf(val);
                    break;
                case serdeConstants.DECIMAL_TYPE_NAME:
                    convertedType = DataType.NUMERIC;
                    convertedValue = isDefaultPartition ? null
                            : HiveDecimal.create(val).bigDecimalValue().toString();
                    break;
                case serdeConstants.VARCHAR_TYPE_NAME:
                    convertedType = DataType.VARCHAR;
                    convertedValue = isDefaultPartition ? null : val;
                    break;
                case serdeConstants.CHAR_TYPE_NAME:
                    convertedType = DataType.BPCHAR;
                    convertedValue = isDefaultPartition ? null : val;
                    break;
                case serdeConstants.BINARY_TYPE_NAME:
                    convertedType = DataType.BYTEA;
                    convertedValue = isDefaultPartition ? null : val.getBytes();
                    break;
                default:
                    throw new UnsupportedTypeException(
                            "Unsupported partition type: " + type);
            }

            if (columnDescriptorContainsColumn(columnName)) {
                partitionColumnNames.put(StringUtils.lowerCase(columnName),
                        new OneField(convertedType.getOID(), convertedValue));
            }
        }
        numberOfPartitions = partitionColumnNames.size();
    }

    /*
     * The partition fields are initialized one time based on userData provided
     * by the fragmenter.
     */
    void initTextPartitionFields(StringBuilder parts) {
        List<HivePartition> hivePartitionList = metadata.getPartitions();
        if (hivePartitionList == null || hivePartitionList.size() == 0) {
            return;
        }
        for (HivePartition partition : hivePartitionList) {
            String type = partition.getType();
            String val = partition.getValue();
            parts.append(delimiter);
            if (isDefaultPartition(type, val)) {
                parts.append(nullChar);
            } else {
                // ignore the type's parameters
                String typeName = type.replaceAll("\\(.*\\)", "");
                switch (typeName) {
                    case serdeConstants.STRING_TYPE_NAME:
                    case serdeConstants.VARCHAR_TYPE_NAME:
                    case serdeConstants.CHAR_TYPE_NAME:
                        parts.append(val);
                        break;
                    case serdeConstants.BOOLEAN_TYPE_NAME:
                        parts.append(Boolean.parseBoolean(val));
                        break;
                    case serdeConstants.TINYINT_TYPE_NAME:
                    case serdeConstants.SMALLINT_TYPE_NAME:
                        parts.append(Short.parseShort(val));
                        break;
                    case serdeConstants.INT_TYPE_NAME:
                        parts.append(Integer.parseInt(val));
                        break;
                    case serdeConstants.BIGINT_TYPE_NAME:
                        parts.append(Long.parseLong(val));
                        break;
                    case serdeConstants.FLOAT_TYPE_NAME:
                        parts.append(Float.parseFloat(val));
                        break;
                    case serdeConstants.DOUBLE_TYPE_NAME:
                        parts.append(Double.parseDouble(val));
                        break;
                    case serdeConstants.TIMESTAMP_TYPE_NAME:
                        parts.append(Timestamp.valueOf(val));
                        break;
                    case serdeConstants.DATE_TYPE_NAME:
                        parts.append(Date.valueOf(val));
                        break;
                    case serdeConstants.DECIMAL_TYPE_NAME:
                        parts.append(HiveDecimal.create(val).bigDecimalValue());
                        break;
                    case serdeConstants.BINARY_TYPE_NAME:
                        Utilities.byteArrayToOctalString(val.getBytes(), parts);
                        break;
                    default:
                        throw new UnsupportedTypeException(
                                "Unsupported partition type: " + type);
                }
            }
        }
        this.numberOfPartitions = hivePartitionList.size();
    }

    /**
     * Returns true if the partition value is Hive's default partition name
     * (defined in hive.exec.default.partition.name).
     *
     * @param partitionType  partition field type
     * @param partitionValue partition value
     * @return true if the partition value is Hive's default partition
     */
    protected boolean isDefaultPartition(String partitionType,
                                         String partitionValue) {
        boolean isDefaultPartition = false;
        if (hiveDefaultPartName.equals(partitionValue)) {
            LOG.debug("partition {} is hive default partition (value {})," +
                    "converting field to NULL", partitionType, partitionValue);
            isDefaultPartition = true;
        }
        return isDefaultPartition;
    }

    /*
     * If the object representing the whole record is null or if an object
     * representing a composite sub-object (map, list,..) is null - then
     * BadRecordException will be thrown. If a primitive field value is null,
     * then a null will appear for the field in the record in the query result.
     * flatten is true only when we are dealing with a non primitive field
     */
    private void traverseTuple(Object obj, ObjectInspector objInspector,
                               List<OneField> record, boolean toFlatten)
            throws IOException, BadRecordException {
        ObjectInspector.Category category = objInspector.getCategory();
        switch (category) {
            case PRIMITIVE:
                resolvePrimitive(obj, (PrimitiveObjectInspector) objInspector,
                        record, toFlatten);
                break;
            case LIST:
                if (obj == null) {
                    addOneFieldToRecord(record, DataType.TEXT, null);
                } else {
                    List<OneField> listRecord = traverseList(obj,
                            (ListObjectInspector) objInspector);
                    addOneFieldToRecord(record, DataType.TEXT, String.format("[%s]",
                            HdfsUtilities.toString(listRecord, collectionDelim)));
                }
                break;
            case MAP:
                if (obj == null) {
                    addOneFieldToRecord(record, DataType.TEXT, null);
                } else {
                    List<OneField> mapRecord = traverseMap(obj,
                            (MapObjectInspector) objInspector);
                    addOneFieldToRecord(record, DataType.TEXT, String.format("{%s}",
                            HdfsUtilities.toString(mapRecord, collectionDelim)));
                }
                break;
            case STRUCT:
                if (obj == null) {
                    addOneFieldToRecord(record, DataType.TEXT, null);
                } else {
                    List<OneField> structRecord = traverseStruct(obj,
                            (StructObjectInspector) objInspector, true);
                    addOneFieldToRecord(record, DataType.TEXT, String.format("{%s}",
                            HdfsUtilities.toString(structRecord, collectionDelim)));
                }
                break;
            case UNION:
                if (obj == null) {
                    addOneFieldToRecord(record, DataType.TEXT, null);
                } else {
                    List<OneField> unionRecord = traverseUnion(obj,
                            (UnionObjectInspector) objInspector);
                    addOneFieldToRecord(record, DataType.TEXT, String.format("[%s]",
                            HdfsUtilities.toString(unionRecord, collectionDelim)));
                }
                break;
            default:
                throw new UnsupportedTypeException("Unknown category type: "
                        + objInspector.getCategory());
        }
    }

    private List<OneField> traverseUnion(Object obj, UnionObjectInspector uoi)
            throws BadRecordException, IOException {
        List<OneField> unionRecord = new LinkedList<>();
        List<? extends ObjectInspector> ois = uoi.getObjectInspectors();
        if (ois == null) {
            throw new BadRecordException(
                    "Illegal value NULL for Hive data type Union");
        }
        traverseTuple(uoi.getField(obj), ois.get(uoi.getTag(obj)), unionRecord,
                true);
        return unionRecord;
    }

    private List<OneField> traverseList(Object obj, ListObjectInspector loi)
            throws BadRecordException, IOException {
        List<OneField> listRecord = new LinkedList<>();
        List<?> list = loi.getList(obj);
        ObjectInspector eoi = loi.getListElementObjectInspector();
        if (list == null) {
            throw new BadRecordException(
                    "Illegal value NULL for Hive data type List");
        }
        for (Object object : list) {
            traverseTuple(object, eoi, listRecord, true);
        }
        return listRecord;
    }

    protected List<OneField> traverseStruct(Object struct,
                                            StructObjectInspector soi,
                                            boolean toFlatten)
            throws BadRecordException, IOException {
        // "fields" represents the projected schema
        List<? extends StructField> fields = soi.getAllStructFieldRefs();
        // structFields contains a list of all values, null for non-projected fields
        // the number of structFields matches the number of columns on the original hive table
        // also the order of the structFields matches the hive table schema
        List<Object> structFields = soi.getStructFieldsDataAsList(struct);
        if (structFields == null) {
            throw new BadRecordException("Illegal value NULL for Hive data type Struct");
        }

        List<OneField> structRecord = new LinkedList<>();
        List<OneField> complexRecord = new LinkedList<>();
        OneField partitionField;

        if (toFlatten) {
            for (int i = 0; i < structFields.size(); i++) {
                complexRecord.add(new OneField(DataType.TEXT.getOID(), String.format("\"%s\"", fields.get(i).getFieldName())));
                traverseTuple(structFields.get(i), fields.get(i).getFieldObjectInspector(), complexRecord, true);
                addOneFieldToRecord(structRecord, DataType.TEXT, HdfsUtilities.toString(complexRecord, mapkeyDelim));
                complexRecord.clear();
            }
        } else {
            Map<String, Integer> columnNameToStructIndexMap =
                    IntStream.range(0, fields.size())
                            .boxed()
                            .collect(Collectors.toMap(i -> StringUtils.lowerCase(fields.get(i).getFieldName()), i -> i));

            List<ColumnDescriptor> tupleDescription = context.getTupleDescription();
            for (int j = 0; j < tupleDescription.size(); j++) {
                ColumnDescriptor columnDescriptor = tupleDescription.get(j);
                String lowercaseColumnName = StringUtils.lowerCase(columnDescriptor.columnName());
                // i is the index of the projected column, this will match j in most
                // cases, but in some cases where projection information is not passed
                // to the deserializer, this will not hold true. Let's consider the case
                // where the hive table is defined as a,b,c,d. The greenplum table
                // contains a subset of the columns and is defined as c,a. Then fields
                // will only have two entries, whereas structFields will still have
                // 4 entries. In this case i will be 0, for the first greenplum column.
                Integer i = columnNameToStructIndexMap.get(lowercaseColumnName);
                // structIndex corresponds to the index of the column on hive
                // for example if the hive table has columns a, b, c, but
                // Greenplum defines them as c, b, a, hiveIndexes will have values
                // 2,1,0. And the value of structIndex for the first greenplum
                // column will be 2
                Integer structIndex = hiveIndexes.get(j);

                if ((partitionField = partitionColumnNames.get(lowercaseColumnName)) != null) {
                    // Skip partitioned columns
                    complexRecord.add(partitionField);
                } else if (i == null || structIndex >= structFields.size()) {
                    // This is a column not present in the file, but defined in greenplum.
                    LOG.warn("Column {} is not present in the source file, but it is defined in the table", columnDescriptor.columnName());
                    addOneFieldToRecord(complexRecord, columnDescriptor.getDataType(), null);
                } else if (!columnDescriptor.isProjected()) {
                    // Non-projected fields will be sent as null values.
                    // This case is invoked only in the top level of fields and
                    // not when interpreting fields of type struct.
                    traverseTuple(null, fields.get(i).getFieldObjectInspector(), complexRecord, false);
                } else {
                    traverseTuple(structFields.get(structIndex), fields.get(i).getFieldObjectInspector(), complexRecord, false);
                }
            }
        }

        return toFlatten ? structRecord : complexRecord;
    }

    private List<OneField> traverseMap(Object obj, MapObjectInspector moi)
            throws BadRecordException, IOException {
        List<OneField> complexRecord = new LinkedList<>();
        List<OneField> mapRecord = new LinkedList<>();
        ObjectInspector koi = moi.getMapKeyObjectInspector();
        ObjectInspector voi = moi.getMapValueObjectInspector();
        Map<?, ?> map = moi.getMap(obj);
        if (map == null) {
            throw new BadRecordException(
                    "Illegal value NULL for Hive data type Map");
        } else if (map.isEmpty()) {
            traverseTuple(null, koi, complexRecord, true);
            traverseTuple(null, voi, complexRecord, true);
            addOneFieldToRecord(mapRecord, DataType.TEXT,
                    HdfsUtilities.toString(complexRecord, mapkeyDelim));
        } else {
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                traverseTuple(entry.getKey(), koi, complexRecord, true);
                traverseTuple(entry.getValue(), voi, complexRecord, true);
                addOneFieldToRecord(mapRecord, DataType.TEXT,
                        HdfsUtilities.toString(complexRecord, mapkeyDelim));
                complexRecord.clear();
            }
        }
        return mapRecord;
    }

    private void resolvePrimitive(Object o, PrimitiveObjectInspector oi,
                                  List<OneField> record, boolean toFlatten) {
        Object val;
        switch (oi.getPrimitiveCategory()) {
            case BOOLEAN: {
                val = (o != null) ? ((BooleanObjectInspector) oi).get(o) : null;
                addOneFieldToRecord(record, DataType.BOOLEAN, val);
                break;
            }
            case SHORT: {
                if (o == null) {
                    val = null;
                } else if (o.getClass().getSimpleName().equals("ByteWritable")) {
                    val = (short) ((ByteWritable) o).get();
                } else {
                    val = ((ShortObjectInspector) oi).get(o);
                }
                addOneFieldToRecord(record, DataType.SMALLINT, val);
                break;
            }
            case INT: {
                val = (o != null) ? ((IntObjectInspector) oi).get(o) : null;
                addOneFieldToRecord(record, DataType.INTEGER, val);
                break;
            }
            case LONG: {
                val = (o != null) ? ((LongObjectInspector) oi).get(o) : null;
                addOneFieldToRecord(record, DataType.BIGINT, val);
                break;
            }
            case FLOAT: {
                val = (o != null) ? ((FloatObjectInspector) oi).get(o) : null;
                addOneFieldToRecord(record, DataType.REAL, val);
                break;
            }
            case DOUBLE: {
                val = (o != null) ? ((DoubleObjectInspector) oi).get(o) : null;
                addOneFieldToRecord(record, DataType.FLOAT8, val);
                break;
            }
            case DECIMAL: {
                String sVal = null;
                if (o != null) {
                    HiveDecimal hd = ((HiveDecimalObjectInspector) oi).getPrimitiveJavaObject(o);
                    if (hd != null) {
                        BigDecimal bd = hd.bigDecimalValue();
                        sVal = bd.toString();
                    }
                }
                addOneFieldToRecord(record, DataType.NUMERIC, sVal);
                break;
            }
            case STRING: {
                val = (o != null) ? ((StringObjectInspector) oi).getPrimitiveJavaObject(o)
                        : null;
                // for more complex types, we need to properly handle special characters by escaping the val
                val = toFlatten
                        ? val != null ? String.format("\"%s\"", StringEscapeUtils.escapeJava(val.toString())) : "null"
                        : val;
                addOneFieldToRecord(record, DataType.TEXT, val);
                break;
            }
            case VARCHAR:
                val = (o != null) ? ((HiveVarcharObjectInspector) oi).getPrimitiveJavaObject(o)
                        : null;
                addOneFieldToRecord(record, DataType.VARCHAR,
                        toFlatten ? String.format("\"%s\"", val) : val);
                break;
            case CHAR:
                val = (o != null) ? ((HiveCharObjectInspector) oi).getPrimitiveJavaObject(o)
                        : null;
                addOneFieldToRecord(record, DataType.BPCHAR,
                        toFlatten ? String.format("\"%s\"", val) : val);
                break;
            case BINARY: {
                byte[] toEncode = null;
                if (o != null) {
                    BytesWritable bw = ((BinaryObjectInspector) oi).getPrimitiveWritableObject(o);
                    toEncode = new byte[bw.getLength()];
                    System.arraycopy(bw.getBytes(), 0, toEncode, 0,
                            bw.getLength());
                }
                addOneFieldToRecord(record, DataType.BYTEA, toEncode);
                break;
            }
            case TIMESTAMP: {
                val = (o != null) ? ((TimestampObjectInspector) oi).getPrimitiveJavaObject(o)
                        : null;
                addOneFieldToRecord(record, DataType.TIMESTAMP, val);
                break;
            }
            case DATE:
                val = (o != null) ? ((DateObjectInspector) oi).getPrimitiveJavaObject(o)
                        : null;
                addOneFieldToRecord(record, DataType.DATE, val);
                break;
            case BYTE: { /* TINYINT */
                val = (o != null) ? (short) ((ByteObjectInspector) oi).get(o)
                        : null;
                addOneFieldToRecord(record, DataType.SMALLINT, val);
                break;
            }
            default: {
                throw new UnsupportedTypeException(oi.getTypeName()
                        + " conversion is not supported by "
                        + getClass().getSimpleName());
            }
        }
    }

    private void addOneFieldToRecord(List<OneField> record,
                                     DataType gpdbWritableType, Object val) {
        record.add(new OneField(gpdbWritableType.getOID(), val));
    }

    /*
     * Gets the delimiter character from the URL, verify and store it. Must be a
     * single ascii character (same restriction as Gpdb's). If a hex
     * representation was passed, convert it to its char.
     */
    void parseDelimiterChar(RequestContext input) {

        String userDelim = input.getGreenplumCSV().getDelimiter() != null ?
                String.valueOf(input.getGreenplumCSV().getDelimiter()) : null;

        if (userDelim == null)
            return;

        final int VALID_LENGTH = 1;
        final int VALID_LENGTH_HEX = 4;
        if (userDelim.startsWith("\\x")) { // hexadecimal sequence
            if (userDelim.length() != VALID_LENGTH_HEX) {
                throw new IllegalArgumentException(
                        "Invalid hexdecimal value for delimiter (got"
                                + userDelim + ")");
            }
            delimiter = (char) Integer.parseInt(
                    userDelim.substring(2, VALID_LENGTH_HEX), 16);
            if (!CharUtils.isAscii(delimiter)) {
                throw new IllegalArgumentException(
                        "Invalid delimiter value. Must be a single ASCII character, or a hexadecimal sequence (got non ASCII "
                                + delimiter + ")");
            }
            return;
        }
        if (userDelim.length() != VALID_LENGTH) {
            throw new IllegalArgumentException(
                    "Invalid delimiter value. Must be a single ASCII character, or a hexadecimal sequence (got "
                            + userDelim + ")");
        }
        if (!CharUtils.isAscii(userDelim.charAt(0))) {
            throw new IllegalArgumentException(
                    "Invalid delimiter value. Must be a single ASCII character, or a hexadecimal sequence (got non ASCII "
                            + userDelim + ")");
        }
        delimiter = userDelim.charAt(0);
    }

    protected Properties getSerdeProperties() {
        return metadata.getProperties();
    }

    private boolean columnDescriptorContainsColumn(String columnName) {
        return context.getTupleDescription()
                .stream()
                .anyMatch(cd -> StringUtils.equalsIgnoreCase(columnName, cd.columnName()));
    }
}
